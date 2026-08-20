package etcdrestart

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/exec"
)

const (
	etcdHealthPollInterval = 5 * time.Second
	etcdHealthTimeout      = 5 * time.Minute
)

// RunTnfEtcdRestart performs a rolling restart of the podman-etcd process on all
// TNF control-plane nodes after a CA bundle rotation. It sets restart_no_leave
// on ALL nodes upfront so that if Pacemaker's monitor triggers a recovery before
// the rolling restart reaches a node, the RA's stop handler still skips
// leave_etcd_member_list().
func RunTnfEtcdRestart() error {
	klog.Info("Setting up clients for TNF etcd-restart")

	clientConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	kubeClient, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create kube client: %w", err)
	}

	// Per-node worst case: 5 min pcs restart --wait + 5 min health poll = 10 min.
	// Two nodes sequentially = 20 min, plus overhead.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	currentNodeName := os.Getenv("MY_NODE_NAME")
	if currentNodeName == "" {
		return fmt.Errorf("MY_NODE_NAME environment variable not set")
	}

	klog.Info("Running TNF etcd-restart")

	// Verify pacemaker cluster is running on this node
	_, _, err = exec.Execute(ctx, "/usr/sbin/pcs cluster status")
	if err != nil {
		return fmt.Errorf("pacemaker cluster not running on this node, will retry on other node: %w", err)
	}

	nodeNames, err := getControlPlaneNodeNames(ctx, kubeClient)
	if err != nil {
		return fmt.Errorf("failed to get control plane node names: %w", err)
	}

	// Set restart_no_leave on ALL nodes BEFORE any restart. This protects against
	// Pacemaker's monitor triggering a recovery (stop+start) before our rolling
	// restart reaches a node — without this attribute, the RA's start operation
	// may set force_new_cluster, causing destructive cluster recovery.
	for _, nodeName := range nodeNames {
		if err := setRestartNoLeave(ctx, nodeName); err != nil {
			return fmt.Errorf("failed to set restart_no_leave on %s: %w", nodeName, err)
		}
	}
	defer func() {
		for _, nodeName := range nodeNames {
			clearRestartNoLeave(ctx, nodeName)
		}
	}()

	// Restart the current node last so etcd stays reachable from the job's API calls
	sortedNames := make([]string, 0, len(nodeNames))
	for _, name := range nodeNames {
		if name != currentNodeName {
			sortedNames = append(sortedNames, name)
		}
	}
	sortedNames = append(sortedNames, currentNodeName)

	for i, nodeName := range sortedNames {
		if err := restartEtcdOnNode(ctx, i+1, len(sortedNames), nodeName); err != nil {
			return fmt.Errorf("failed to restart etcd on node %d/%d: %w", i+1, len(sortedNames), err)
		}
	}

	klog.Info("Rolling etcd restart completed successfully on all nodes")
	return nil
}

// restartEtcdOnNode restarts etcd on the given node and waits for health.
// restart_no_leave is already set on all nodes by the caller.
func restartEtcdOnNode(ctx context.Context, nodeIdx, nodeCount int, nodeName string) error {
	nodeLabel := fmt.Sprintf("node %d/%d (%s)", nodeIdx, nodeCount, nodeName)
	klog.Infof("Restarting etcd on %s", nodeLabel)

	cmd := fmt.Sprintf("/usr/sbin/pcs resource restart etcd-clone %s --wait=300", nodeName)
	if _, _, err := exec.Execute(ctx, cmd); err != nil {
		return fmt.Errorf("pcs resource restart failed on %s: %w", nodeLabel, err)
	}

	klog.Infof("etcd restarted on %s, waiting for health", nodeLabel)

	if err := waitForEtcdHealthy(ctx); err != nil {
		return fmt.Errorf("etcd did not become healthy after restart on %s: %w", nodeLabel, err)
	}

	klog.Infof("etcd healthy on %s", nodeLabel)
	return nil
}

func setRestartNoLeave(ctx context.Context, nodeName string) error {
	cmd := fmt.Sprintf(`crm_attribute --lifetime reboot --node %s --name "restart_no_leave" --update "true"`, nodeName)
	if _, _, err := exec.Execute(ctx, cmd); err != nil {
		return fmt.Errorf("failed to set restart_no_leave on %s: %w", nodeName, err)
	}
	klog.Infof("Set restart_no_leave on %s", nodeName)
	return nil
}

func clearRestartNoLeave(ctx context.Context, nodeName string) {
	cmd := fmt.Sprintf(`crm_attribute --lifetime reboot --node %s --name "restart_no_leave" --delete`, nodeName)
	if _, _, err := exec.Execute(ctx, cmd); err != nil {
		klog.Warningf("failed to clear restart_no_leave on %s: %v", nodeName, err)
	}
}

// waitForEtcdHealthy polls etcd endpoint health via podman exec.
func waitForEtcdHealthy(ctx context.Context) error {
	return wait.PollUntilContextTimeout(ctx, etcdHealthPollInterval, etcdHealthTimeout, true, func(ctx context.Context) (bool, error) {
		stdout, _, err := exec.Execute(ctx, "podman exec etcd /usr/bin/etcdctl endpoint health --cluster")
		if err != nil {
			klog.V(4).Infof("etcd health check not yet passing: %v", err)
			return false, nil
		}
		if strings.Contains(stdout, "is healthy") {
			return true, nil
		}
		return false, nil
	})
}

func getControlPlaneNodeNames(ctx context.Context, kubeClient kubernetes.Interface) ([]string, error) {
	nodes, err := kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "node-role.kubernetes.io/master",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list control plane nodes: %w", err)
	}
	if len(nodes.Items) == 0 {
		return nil, fmt.Errorf("no control plane nodes found")
	}

	names := make([]string, len(nodes.Items))
	for i, node := range nodes.Items {
		names[i] = node.Name
	}
	sort.Strings(names)
	return names, nil
}
