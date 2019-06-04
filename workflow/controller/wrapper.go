package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/argoproj/argo/errors"
	"github.com/argoproj/argo/pkg/apis/workflow/v1alpha1"
	wfv1 "github.com/argoproj/argo/pkg/apis/workflow/v1alpha1"
	wfclientset "github.com/argoproj/argo/pkg/client/clientset/versioned"
	"github.com/argoproj/argo/util/file"
	"github.com/argoproj/argo/workflow/common"
	log "github.com/sirupsen/logrus"
	apiv1 "k8s.io/api/core/v1"
	policy_v1beta1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sort"
	"strings"
	"time"
)

var workflowRecordLimitMap = make(map[string]int)

const (
	CustomBindingSuffix = "-restrictedbinding"
)

type AliyunExtraConfig struct {
	EnableHostNetwork bool   `json:"enableHostNetwork,omitempty"`
	DefaultDnsPolicy  string `json:"defaultDnsPolicy,omitempty"`
}

//const aliyunRetryKey = "aliyun.retry"

// ReSyncWorkflow lookup all workflow on incorrect status
func (wfc *WorkflowController) ReSyncWorkflow(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("ReSyncWorkflow panic error %v", r)
		}
	}()

	for {
		listOpt := v1.ListOptions{}
		allWfObjList, err := wfc.wfclientset.ArgoprojV1alpha1().Workflows("").List(listOpt)
		log.Info("ReSyncWorkflow begin to list all workflow")
		if err != nil {
			log.Errorf("ReSyncWorkflow list all workflow error %v", err)
			time.Sleep(1 * time.Minute)
			continue
		}

		conditionTimes := 2

		for _, wf := range allWfObjList.Items {
			if wf.Status.Phase != v1alpha1.NodeRunning {
				continue
			}

			if wrapperGetSize(&wf) >= 500*1024 {
				conditionTimes = 60
			} else {
				conditionTimes = 2
			}

			if tmpErr := wrapperCheckAndDecompress(&wf); tmpErr != nil {
				log.Error(tmpErr)
			}
			checkDagWorkflowStatus(&wf, wfc.wfclientset, wfc.kubeclientset, conditionTimes)
		}

		time.Sleep(1 * time.Minute)
	}

}

func checkDagWorkflowStatus(wf *v1alpha1.Workflow, argoClient wfclientset.Interface, kubeClient kubernetes.Interface,
	conditionTimes int) {
	if wf == nil {
		return
	}

	var targetNode []v1alpha1.NodeStatus
	count := 0
	for _, node := range wf.Status.Nodes {
		if node.Phase == v1alpha1.NodeRunning {
			targetNode = append(targetNode, node)
			count++
		}
		if count > 1 {
			break
		}
	}

	if count > 1 {
		return
	}

	if len(targetNode) != 1 {
		return
	}

	if targetNode[0].Type != v1alpha1.NodeTypeDAG {
		return
	}

	dagNode, ok := wf.Status.Nodes[targetNode[0].ID]
	if !ok {
		return
	}

	mapKey := fmt.Sprintf("%s-%s", wf.Namespace, wf.Name)
	if val, ok := workflowRecordLimitMap[mapKey]; ok {
		if val < conditionTimes {
			workflowRecordLimitMap[mapKey] += 1
			return
		}
	} else {
		workflowRecordLimitMap[mapKey] = 1
		return
	}

	//retryArgoInterface := argoClient.ArgoprojV1alpha1().Workflows(wf.Namespace)
	log.Infof("ReSync Workflow %s:%s status from %s to %s", wf.Namespace, wf.Name, wf.Status.Phase, v1alpha1.NodeFailed)
	dagNode.Phase = v1alpha1.NodeFailed
	wf.Status.Nodes[targetNode[0].ID] = dagNode
	wf.Status.Phase = v1alpha1.NodeFailed

	if tmpErr := wrapperCheckAndCompress(wf); tmpErr != nil {
		log.Error(tmpErr)
	}
	_, err := argoClient.ArgoprojV1alpha1().Workflows(wf.Namespace).Update(wf)
	if err != nil {
		log.Errorf("Update workflow %s:%s error %v", wf.Namespace, wf.Name, err)
	}
	delete(workflowRecordLimitMap, mapKey)

	/*
		wfLabelsMap := wf.Labels
		wfRetryTimesInt := 0
		if retryTimesStr, ok := wfLabelsMap[aliyunRetryKey]; !ok {
			wfLabelsMap[aliyunRetryKey] = "1"
		} else {
			tmpRetryInt, tmpErr := strconv.Atoi(retryTimesStr)
			if tmpErr != nil {
				log.Error(tmpErr)
				tmpRetryInt = 0
			}
			tmpRetryInt++
			wfLabelsMap[aliyunRetryKey] = strconv.Itoa(tmpRetryInt)
			wfRetryTimesInt = tmpRetryInt
		}
		wf.SetLabels(wfLabelsMap)

		time.Sleep(10 * time.Second)
		if wfRetryTimesInt > 3 {
			return
		}
		wf, err = retryArgoInterface.Get(wf.Name, v1.GetOptions{})
		if err != nil {
			log.Errorf("Get workflow %s:%s error %v",  wf.Namespace, wf.Name, err)
			return
		}
		log.Infof("Retry workflow %s:%s for unstable status map value %d", wf.Namespace, wf.Name, workflowRecordLimitMap[mapKey])
		if _, tmpErr := util.RetryWorkflow(kubeClient, retryArgoInterface, wf); tmpErr != nil {
			log.Errorf("Retry workflow %s error %v", wf.Name, tmpErr)
		}
	*/
}

func wrapperCheckAndDecompress(wf *wfv1.Workflow) error {
	if wf.Status.CompressedNodes != "" {
		nodeContent, err := file.DecodeDecompressString(wf.Status.CompressedNodes)
		if err != nil {
			return err
		}
		err = json.Unmarshal([]byte(nodeContent), &wf.Status.Nodes)
		if err != nil {
			return err
		}
	}
	return nil
}

func wrapperCheckAndCompress(wf *wfv1.Workflow) error {

	if wf.Status.CompressedNodes != "" {

		nodeContent, err := json.Marshal(wf.Status.Nodes)
		if err != nil {
			return errors.InternalWrapError(err)
		}
		buff := string(nodeContent)
		wf.Status.CompressedNodes = file.CompressEncodeString(buff)
		wf.Status.Nodes = nil
	}

	return nil
}

// getSize return the entire workflow json string size
func wrapperGetSize(wf *wfv1.Workflow) int {
	nodeContent, err := json.Marshal(wf)
	if err != nil {
		return -1
	}

	compressNodeSize := len(wf.Status.CompressedNodes)

	if compressNodeSize > 0 {
		nodeStatus, err := json.Marshal(wf.Status.Nodes)
		if err != nil {
			return -1
		}
		return len(nodeContent) - len(nodeStatus)
	}
	return len(nodeContent)
}

func (woc *wfOperationCtx) useNonRootExec(tmpl *wfv1.Template) bool {
	if tmpl.SecurityContext != nil {
		woc.log.Infof("newWaitContainer: add securityContext in tmpl (%++v)", *tmpl.SecurityContext)
		//paas the runAsUser id into wf container
		if *tmpl.SecurityContext.RunAsUser > 0 {
			return true
		}
	}
	return woc.controller.Config.NonRootExecutor
}

func (woc *wfOperationCtx) replaceSideCarForNonRoot(tmpl *wfv1.Template, ctr *apiv1.Container) *apiv1.Container {
	if woc.useNonRootExec(tmpl) {
		permissionInt := int64(common.NonrootArgoExecUid)
		if ctr.SecurityContext != nil {
			ctr.SecurityContext.RunAsUser = &permissionInt
		} else {
			sc := &apiv1.SecurityContext{
				RunAsUser: &permissionInt,
			}
			ctr.SecurityContext = sc
		}
		//use custom noroot argoexec image if wf config RunAsUser within psp validation
		ctr.Image = woc.controller.Config.NonRootExecutorImage
		ctr.Command = []string{"/bin/exec.sh"}
		ctr.Args = []string{}
	}
	return ctr
}

func (woc *wfOperationCtx) findGourpLimitInPsp(namespace string) (int64, error) {
	var group int64
	restConfig := woc.controller.restConfig

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return group, errors.InternalWrapError(err)
	}

	rbList, err := clientset.RbacV1().RoleBindings(namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return group, errors.InternalWrapError(err)
	}
	for _, rb := range rbList.Items {
		if strings.HasSuffix(rb.Name, CustomBindingSuffix) {
			refClusterRole := rb.RoleRef.Name
			woc.log.Infof("findGourpLimitInPsp: found the binding clusterrole (%s) in namespace %s", refClusterRole, namespace)

			group, err = getGroupIdInBindingPsp(clientset, refClusterRole)
			if err != nil {
				woc.log.Infof("findGourpLimitInPsp: found the binding clusterrole (%s) in namespace %s", refClusterRole, namespace)
			} else if group > 0 {
				return group, nil
			}
		}
	}

	return group, nil
}

func StringInSlice(targetStr string, strlist []string) bool {
	for _, s := range strlist {
		if s == targetStr {
			return true
		}
	}
	return false
}

func getGroupIdInBindingPsp(clientset *kubernetes.Clientset, refClusterRole string) (int64, error) {
	var group int64
	cr, err := clientset.RbacV1().ClusterRoles().Get(refClusterRole, meta_v1.GetOptions{})
	if err != nil {
		return group, errors.InternalWrapError(err)
	}
	var pspNames []string
	for _, rule := range cr.Rules {
		if StringInSlice("podsecuritypolicies", rule.Resources) && StringInSlice("use", rule.Verbs) {
			pspNames = rule.ResourceNames
			break
		}
	}
	if len(pspNames) > 0 {
		sort.Strings(pspNames)
		for _, pspName := range pspNames {
			psp, err := clientset.Policy().PodSecurityPolicies().Get(pspName, meta_v1.GetOptions{})
			if err != nil {
				return group, errors.InternalWrapError(err)
			}
			if psp.Spec.FSGroup.Size() > 0 && (psp.Spec.FSGroup.Rule == policy_v1beta1.FSGroupStrategyMustRunAs) {
				for _, fsRange := range psp.Spec.FSGroup.Ranges {
					return fsRange.Min, nil
				}
			}
		}
	}
	return group, nil
}

func (woc *wfOperationCtx) dialWithSecurityContext(mainCtr apiv1.Container, tmpl *wfv1.Template) apiv1.Container {
	if tmpl.SecurityContext != nil {
		woc.log.Infof("createWorkflowPod: add SecurityContext in tmpl (%v)", *tmpl.SecurityContext)
		//paas the runAsUser id into wf container
		if *tmpl.SecurityContext.RunAsUser > 0 {
			sc := &apiv1.SecurityContext{
				RunAsUser: tmpl.SecurityContext.RunAsUser,
			}
			mainCtr.SecurityContext = sc
		}
	}
	//TODO replace this if RunAsGroup supported in psp policy meta (>= 1.13)
	gid, err := woc.findGourpLimitInPsp(woc.wf.ObjectMeta.Namespace)
	if err != nil {
		woc.log.Warningf("error occur when query wf runtime gid, err: %v", err)
	} else if gid != 0 {
		woc.log.Infof("add 'RunAsGroup' %s into sc for template %s, gid %d", tmpl.Name, gid)
		mainCtr.SecurityContext.RunAsGroup = &gid
	} else {
		woc.log.Warningf("no gid found in binding psp for given template %s, namespace %s", tmpl.Name, woc.wf.ObjectMeta.Namespace)
	}

	return mainCtr
}

func (woc *wfOperationCtx) UpdateAliyunExtraConfig(pod *apiv1.Pod) {
	if pod == nil {
		return
	}
	if woc.controller.Config.ExtraConfig.EnableHostNetwork {
		pod.Spec.HostNetwork = true
	}

	dnsPolicy := woc.controller.Config.ExtraConfig.DefaultDnsPolicy
	if dnsPolicy == "" {
		return
	}
	dnsPolicyMap := map[string]bool{
		string(apiv1.DNSClusterFirstWithHostNet): true,
		string(apiv1.DNSClusterFirst):            true,
		string(apiv1.DNSDefault):                 true,
		string(apiv1.DNSNone):                    true,
	}
	if ok := dnsPolicyMap[dnsPolicy]; ok {
		pod.Spec.DNSPolicy = apiv1.DNSPolicy(dnsPolicy)
	} else {
		log.Warningf("UpdateAliyunExtraConfig pod name %s dnsPolicy %s is inValidate", pod.Name, dnsPolicy)
	}
}
