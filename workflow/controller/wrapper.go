package controller

import (
	"context"
	"fmt"
	"github.com/argoproj/argo/pkg/apis/workflow/v1alpha1"
	wfclientset "github.com/argoproj/argo/pkg/client/clientset/versioned"
	"github.com/argoproj/argo/workflow/util"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"strconv"
	"time"
)

var workflowRecordLimitMap = make(map[string]int)
const aliyunRetryKey = "aliyun.retry"

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

		for _, wf := range allWfObjList.Items {
			if wf.Status.Phase != v1alpha1.NodeRunning {
				continue
			}

			checkDagWorkflowStatus(&wf, wfc.wfclientset, wfc.kubeclientset)
		}

		time.Sleep(5 * time.Minute)
	}

}

func checkDagWorkflowStatus(wf *v1alpha1.Workflow, argoClient wfclientset.Interface, kubeClient kubernetes.Interface) {
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
		if val < 3 {
			workflowRecordLimitMap[mapKey] += 1
			return
		}
	} else {
		workflowRecordLimitMap[mapKey] = 1
		return
	}

	retryArgoInterface := argoClient.ArgoprojV1alpha1().Workflows(wf.Namespace)
	log.Infof("ReSync Workflow %s:%s status from %s to %s", wf.Namespace, wf.Name, wf.Status.Phase, v1alpha1.NodeFailed)
	dagNode.Phase = v1alpha1.NodeFailed
	wf.Status.Nodes[targetNode[0].ID] = dagNode
	wf.Status.Phase = v1alpha1.NodeFailed

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

	_, err := argoClient.ArgoprojV1alpha1().Workflows(wf.Namespace).Update(wf)
	if err != nil {
		log.Errorf("Update workflow %s:%s error %v", wf.Namespace, wf.Name, err)
	}

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

	delete(workflowRecordLimitMap, mapKey)
}
