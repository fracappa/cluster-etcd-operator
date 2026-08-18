package etcdrestart

import (
	"context"
	"crypto/sha256"
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

	"github.com/openshift/cluster-etcd-operator/pkg/operator/operatorclient"
	"github.com/openshift/cluster-etcd-operator/pkg/tlshelpers"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/exec"
)

const (
	etcdHealthPollInterval = 5 * time.Second
	etcdHealthTimeout      = 5 * time.Minute
	certSyncPollInterval   = 10 * time.Second
	certSyncTimeout        = 10 * time.Minute

	caBundleDiskDir = "/etc/kubernetes/static-pod-resources/etcd-certs/configmaps/etcd-all-bundles"
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

	// Per-node worst case: 10 min cert sync + 5 min pcs restart + 5 min health = 20 min.
	// Two nodes sequentially = ~40 min worst case, but cert sync is shared.
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

	// Wait for cert files on disk to match the ConfigMap before restarting.
	// The ConfigMap update triggers this job, but the revision controller
	// syncs the files to disk asynchronously.
	if err := waitForCertSync(ctx, kubeClient); err != nil {
		return fmt.Errorf("cert files not synced to disk: %w", err)
	}

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

	// Clear stale "Failed Resource Actions" left by monitor probes that ran
	// before the restart (e.g. TLS errors from the old CA bundle).
	if _, _, err := exec.Execute(ctx, "/usr/sbin/pcs resource cleanup etcd-clone"); err != nil {
		klog.Warningf("Failed to cleanup etcd-clone resource history: %v", err)
	}

	klog.Info("Rolling etcd restart completed successfully on all nodes")
	return nil
}

// restartEtcdOnNode restarts etcd on the given node and waits for health.
// restart_no_leave is already set on all nodes by the caller.
func restartEtcdOnNode(ctx context.Context, nodeIdx, nodeCount int, nodeName string) error {
	nodeLabel := fmt.Sprintf("node %d/%d", nodeIdx, nodeCount)
	klog.Infof("Restarting etcd on %s", nodeLabel)

	// Restart etcd on the target node. --wait blocks until the resource has
	// stopped and started again (timeout 300s = 5 min).
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

// waitForCertSync polls until the CA bundle on disk matches the etcd-all-bundles
// ConfigMap content. The ConfigMap update triggers the restart job, but the
// revision controller syncs files to disk asynchronously.
func waitForCertSync(ctx context.Context, kubeClient kubernetes.Interface) error {
	klog.Info("Waiting for CA bundle on disk to match ConfigMap")

	return wait.PollUntilContextTimeout(ctx, certSyncPollInterval, certSyncTimeout, true, func(ctx context.Context) (bool, error) {
		expectedHash, err := getConfigMapBundleHash(ctx, kubeClient)
		if err != nil {
			klog.V(4).Infof("Failed to get ConfigMap bundle hash: %v", err)
			return false, nil
		}

		diskHash, err := getDiskBundleHash(ctx)
		if err != nil {
			klog.V(4).Infof("Failed to get disk bundle hash: %v", err)
			return false, nil
		}

		if expectedHash == diskHash {
			klog.Infof("CA bundle on disk matches ConfigMap (hash: %s)", expectedHash[:12])
			return true, nil
		}

		klog.V(4).Infof("CA bundle not yet synced to disk (ConfigMap: %s, disk: %s)", expectedHash[:12], diskHash[:12])
		return false, nil
	})
}

func getConfigMapBundleHash(ctx context.Context, kubeClient kubernetes.Interface) (string, error) {
	cm, err := kubeClient.CoreV1().ConfigMaps(operatorclient.TargetNamespace).Get(ctx, tlshelpers.EtcdAllBundlesConfigMapName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get ConfigMap %s: %w", tlshelpers.EtcdAllBundlesConfigMapName, err)
	}

	h := sha256.New()
	keys := make([]string, 0, len(cm.Data))
	for k := range cm.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte(cm.Data[k]))
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func getDiskBundleHash(ctx context.Context) (string, error) {
	// List files in the bundle directory, sorted alphabetically (same order as ConfigMap keys)
	stdout, _, err := exec.Execute(ctx, fmt.Sprintf("ls -1 %s", caBundleDiskDir))
	if err != nil {
		return "", fmt.Errorf("failed to list bundle directory: %w", err)
	}

	files := strings.Split(strings.TrimSpace(stdout), "\n")
	sort.Strings(files)

	h := sha256.New()
	for _, file := range files {
		if file == "" {
			continue
		}
		content, _, err := exec.Execute(ctx, fmt.Sprintf("cat %s/%s", caBundleDiskDir, file))
		if err != nil {
			return "", fmt.Errorf("failed to read %s: %w", file, err)
		}
		h.Write([]byte(file))
		h.Write([]byte(content))
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
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
