package common

import (
	"github.com/argoproj/argo/errors"
	wfv1 "github.com/argoproj/argo/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo/pkg/client/clientset/versioned/typed/workflow/v1alpha1"
	log "github.com/sirupsen/logrus"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/typed/core/v1"
	"strings"
	"time"
)

func retryFromAssignStep(kubeClient kubernetes.Interface, wfClient v1alpha1.WorkflowInterface, wf *wfv1.Workflow,
	stepName string) (*wfv1.Workflow, error) {
	validateRetryCondition(wf)

	newWF := wf.DeepCopy()
	podIf := kubeClient.CoreV1().Pods(wf.ObjectMeta.Namespace)

	// Delete/reset fields which indicate workflow completed
	delete(newWF.Labels, LabelKeyCompleted)
	delete(newWF.Labels, LabelKeyPhase)
	newWF.Status.Phase = wfv1.NodeRunning
	newWF.Status.Message = ""
	newWF.Status.FinishedAt = metav1.Time{}

	// Iterate the previous nodes. If it was successful Pod carry it forward
	var targetNode wfv1.NodeStatus
	tmpNodeStatus := make(map[string]wfv1.NodeStatus)
	for key, value := range wf.Status.Nodes {
		tmpNodeStatus[key] = value
	}

	deletePolicy := metav1.DeletePropagationForeground

	for _, node := range wf.Status.Nodes {
		if node.DisplayName == stepName {
			targetNode = node
			delete(tmpNodeStatus, node.ID)
			if node.Type == wfv1.NodeTypePod {
				log.Infof("Deleting pod: %s", node.ID)
				err := podIf.Delete(node.ID, &metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
				if err != nil && !apierr.IsNotFound(err) {
					return nil, errors.InternalWrapError(err)
				}
				waitPodDeleted(podIf, node)
			}
			break
		}
	}
	if targetNode.ID == "" {
		log.Fatalf("Can't find step name %s", stepName)
	}

	//Delete owner StepGroup and Steps
	for key, node := range wf.Status.Nodes {
		if len(node.Children) > 0 && node.Children[0] == targetNode.ID {
			delete(tmpNodeStatus, key)
		}

		if node.Type == wfv1.NodeTypeSteps {
			delete(tmpNodeStatus, key)
			continue
		}
	}

	loopDeleteChildNode(tmpNodeStatus, targetNode, wf, podIf)

	newWF.Status.Nodes = tmpNodeStatus
	newWF, err := wfClient.Update(newWF)
	if err != nil {
		log.Fatal(err)
	}
	return newWF, nil
}

func loopDeleteChildNode(nodeStatus map[string]wfv1.NodeStatus, targetNode wfv1.NodeStatus, wf *wfv1.Workflow,
	podIf v1.PodInterface) {
	if len(targetNode.Children) <= 0 {
		return
	}

	childID := targetNode.Children[0]
	delete(nodeStatus, childID)

	var tmpTargetNode wfv1.NodeStatus
	for _, node := range wf.Status.Nodes {
		if node.ID == childID {
			tmpTargetNode = node
			break
		}
	}

	if tmpTargetNode.ID == "" {
		return
	}

	deletePolicy := metav1.DeletePropagationForeground

	if tmpTargetNode.Type == wfv1.NodeTypePod {
		log.Infof("Deleting pod: %s", tmpTargetNode.ID)
		err := podIf.Delete(tmpTargetNode.ID, &metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
		if err != nil && !apierr.IsNotFound(err) {
			log.Warnf("Deleting pod %s", tmpTargetNode.ID)
		}
		waitPodDeleted(podIf, tmpTargetNode)
	}

	loopDeleteChildNode(nodeStatus, tmpTargetNode, wf, podIf)
}

func waitPodDeleted(podIf v1.PodInterface, tmpTargetNode wfv1.NodeStatus) {

	count := 1
	for true {
		tmpPod, err := podIf.Get(tmpTargetNode.ID, metav1.GetOptions{})
		if apierr.IsNotFound(err) {
			break
		}
		if tmpPod.Name == "" {
			break
		}
		count++
		if count > 100 {
			log.Errorf("Wait pod %s delete timeout", tmpTargetNode.ID)
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func validateRetryCondition(wf *wfv1.Workflow) bool {
	for _, node := range wf.Status.Nodes {
		if node.Type == wfv1.NodeTypeDAG {
			log.Fatal("Retry from assign step don't support DAG type workflow")
			return false
		}

		if node.Type == wfv1.NodeTypeStepGroup {
			if len(node.Children) > 1 {
				log.Fatal("Retry from assign step don't support parallel workflow")
				return false
			}
		}
	}

	return true
}

func ReplaceTemplateName(wf *wfv1.Workflow) *wfv1.Workflow {
	if wf == nil {
		return nil
	}

	if strings.Contains(wf.Spec.Entrypoint, "_") {
		wf.Spec.Entrypoint = strings.Replace(wf.Spec.Entrypoint, "_", "-", -1)
	}

	if strings.Contains(wf.GenerateName, "_") {
		wf.GenerateName = strings.Replace(wf.GenerateName, "_", "-", -1)
	}
	wf.GenerateName = strings.ToLower(wf.GenerateName)

	for index, tm := range wf.Spec.Templates {
		if strings.Contains(tm.Name, "_") {
			tm.Name = strings.Replace(tm.Name, "_", "-", -1)
		}
		for tsIndex, stepGroup := range tm.Steps {
			for sgIndex, step := range stepGroup {
				if strings.Contains(step.Template, "_") {
					step.Template = strings.Replace(step.Template, "_", "-", -1)
				}
				stepGroup[sgIndex] = step
			}
			tm.Steps[tsIndex] = stepGroup
		}

		if tm.DAG != nil {
			for index, task := range tm.DAG.Tasks {
				if strings.Contains(task.Template, "_") {
					task.Template = strings.Replace(task.Template, "_", "-", -1)
				}
				tm.DAG.Tasks[index] = task
			}
		}

		wf.Spec.Templates[index] = tm
	}

	return nil
}
