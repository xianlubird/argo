package controller

import (
	"context"
	"github.com/argoproj/argo/pkg/apis/workflow/v1alpha1"
	wfclientset "github.com/argoproj/argo/pkg/client/clientset/versioned"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

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

			checkDagWorkflowStatus(&wf, wfc.wfclientset)
		}

		time.Sleep(5 * time.Minute)
	}

}

func checkDagWorkflowStatus(wf *v1alpha1.Workflow, argoClient wfclientset.Interface) {
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

	log.Infof("ReSync Workflow %s:%s status from %s to %s", wf.Namespace, wf.Name, wf.Status.Phase, v1alpha1.NodeFailed)
	dagNode.Phase = v1alpha1.NodeFailed
	wf.Status.Nodes[targetNode[0].ID] = dagNode
	wf.Status.Phase = v1alpha1.NodeFailed
	_, err := argoClient.ArgoprojV1alpha1().Workflows(wf.Namespace).Update(wf)
	if err != nil {
		log.Errorf("Update workflow %s:%s error %v", wf.Namespace, wf.Name, err)
	}
}
