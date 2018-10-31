package commands

import (
	"fmt"
	wfv1 "github.com/argoproj/argo/pkg/apis/workflow/v1alpha1"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"strconv"
)

func NewTopCommand() *cobra.Command {
	var command = &cobra.Command{
		Use:   "top Resource (CPU/Memory) usage of a workflow.",
		Short: "display resource (CPU/Memory) usage of a workflow.",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				cmd.HelpFunc()(cmd, args)
				os.Exit(1)
			}
			kubeClient := initKubeClient()
			wfClient := InitWorkflowClient()
			wf, err := wfClient.Get(args[0], metav1.GetOptions{})
			if err != nil {
				log.Fatal(err)
			}
			getPodResource(wf, kubeClient)
		},
	}

	return command
}

func getPodResource(wf *wfv1.Workflow, kubeClient *kubernetes.Clientset) (float64, float64, float64, float64) {
	if wf == nil {
		log.Fatal("Wf is nil")
	}
	//unit  cpu/minute
	cpuMax := 0.0
	cpuMin := 0.0
	memoryMax := 0.0
	memoryMin := 0.0

	for _, node := range wf.Status.Nodes {
		if node.Type == wfv1.NodeTypePod {

			finishTime := node.FinishedAt.Time
			if finishTime.IsZero() {
				continue
			}
			startTime := node.StartedAt
			subM := finishTime.Sub(startTime.Time)
			hours := subM.Hours()

			templateName := node.TemplateName
			for _, tmpTemplate := range wf.Spec.Templates {
				if tmpTemplate.Name == templateName && tmpTemplate.Container != nil {
					container := tmpTemplate.Container
					requestCPU := container.Resources.Requests.Cpu().MilliValue()
					requestMemory := container.Resources.Requests.Memory().Value()
					limitCPU := container.Resources.Limits.Cpu().MilliValue()
					limitMemory := container.Resources.Limits.Memory().Value()
					cpuMin += float64(requestCPU) * hours
					cpuMax += float64(limitCPU) * hours
					memoryMin += float64(requestMemory) * hours
					memoryMax += float64(limitMemory) * hours
				}
			}
		}
	}

	cpuMax /= 1000
	cpuMin /= 1000
	memoryMax /= 1024 * 1024 * 1024
	memoryMin /= 1024 * 1024 * 1024
	return Decimal(cpuMax), Decimal(cpuMin), Decimal(memoryMin), Decimal(memoryMax)
}

func Decimal(value float64) float64 {
	value, _ = strconv.ParseFloat(fmt.Sprintf("%.5f", value), 64)
	return value
}

func getCpuMemoryRequest(node wfv1.NodeStatus, namespace string, kubeClient *kubernetes.Clientset, wf *wfv1.Workflow) (float64, float64) {
	if node.Type != wfv1.NodeTypePod {
		return 0, 0
	}
	finishTime := node.FinishedAt.Time
	if finishTime.IsZero() {
		return 0, 0
	}
	startTime := node.StartedAt
	subM := finishTime.Sub(startTime.Time)
	hours := subM.Hours()

	for _, tmpTemplate := range wf.Spec.Templates {
		if tmpTemplate.Name == node.TemplateName && tmpTemplate.Container != nil {
			container := tmpTemplate.Container
			cpus := container.Resources.Requests.Cpu().MilliValue()
			cpusFloat := float64(cpus)
			cpusFloat /= 1000
			cpusFloat = Decimal(cpusFloat * hours)
			memory := container.Resources.Requests.Memory().Value()
			memoryFloat := float64(memory)
			memoryFloat /= 1024 * 1024 * 1024
			memoryFloat = Decimal(memoryFloat * hours)
			return cpusFloat, memoryFloat
		}
	}

	return 0, 0
}

func SetClientConfig(client clientcmd.ClientConfig) {
	clientConfig = client
}

func ParseLogFlagFromParent(cmd *cobra.Command) (containerName string, workflow bool, follow bool, since string,
	sinceTime string, tail int64, timestamps bool) {
	flag := cmd.Flags()
	if flag == nil {
		return
	}

	containerName = "main"
	workflow = false
	follow = false
	since = ""
	sinceTime = ""
	tail = -1
	timestamps = false

	var err error
	if containerName, err = flag.GetString("container"); err != nil {
		log.Error(err)
	}
	if workflow, err = flag.GetBool("workflow"); err != nil {
		log.Error(err)
	}
	if follow, err = flag.GetBool("follow"); err != nil {
		log.Error(err)
	}
	if since, err = flag.GetString("since"); err != nil {
		log.Error(err)
	}
	if sinceTime, err = flag.GetString("since-time"); err != nil {
		log.Error(err)
	}
	if tail, err = flag.GetInt64("tail"); err != nil {
		log.Error(err)
	}
	if timestamps, err = flag.GetBool("timestamps"); err != nil {
		log.Error(err)
	}
	return
}
