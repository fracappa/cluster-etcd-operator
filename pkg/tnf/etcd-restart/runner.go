package etcdrestart

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
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
	etcdHealthPollInterval  = 5 * time.Second
	etcdHealthTimeout       = 5 * time.Minute
	certSyncPodPollInterval = 5 * time.Second
	certSyncPodTimeout      = 2 * time.Minute
	certSyncPollInterval    = 10 * time.Second
	certSyncTimeout         = 5 * time.Minute

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

	// Read the ConfigMap ONCE while the API is still reachable. After CA
	// rotation, etcd's TLS breaks (etcd 3.6 hot-reloads leaf certs but not
	// --trusted-ca-file), making the kube API unavailable for subsequent reads.
	bundleData, expectedHash, err := readBundleConfigMap(ctx, kubeClient)
	if err != nil {
		return fmt.Errorf("failed to read etcd-all-bundles ConfigMap: %w", err)
	}
	klog.Infof("Cached etcd-all-bundles ConfigMap (hash: %s, %d keys)", expectedHash[:12], len(bundleData))

	nodeNames, err := getControlPlaneNodeNames(ctx, kubeClient)
	if err != nil {
		return fmt.Errorf("failed to get control plane node names: %w", err)
	}

	// Sync cert files to ALL nodes before any restart. After CA rotation,
	// etcd 3.6 hot-reloads leaf certs but not --trusted-ca-file, breaking TLS.
	// The revision controller syncs files asynchronously and may be blocked by
	// the outage itself (chicken-and-egg). Creating a pod on each node that
	// writes the cached ConfigMap data to the host disk breaks this deadlock
	// and eliminates the cross-node cert sync race.
	if err := syncCertFilesToAllNodes(ctx, kubeClient, nodeNames, expectedHash, bundleData); err != nil {
		return fmt.Errorf("failed to sync cert files to nodes: %w", err)
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

	// Clear stale "Failed Resource Actions" left by monitor probes that ran
	// before the restart (e.g. TLS errors from the old CA bundle).
	if _, _, err := exec.Execute(ctx, "/usr/sbin/pcs resource cleanup etcd-clone"); err != nil {
		klog.Warningf("Failed to cleanup etcd-clone resource history: %v", err)
	}

	klog.Info("Rolling etcd restart completed successfully on all nodes")
	return nil
}

// syncCertFilesToAllNodes ensures the CA bundle files are written to the host
// disk on every control-plane node. It first tries creating a cert-sync pod on
// each node (reliable, works cross-node). If that fails, it falls back to a
// local-only sync (checks disk hash, or writes directly via nsenter).
func syncCertFilesToAllNodes(ctx context.Context, kubeClient kubernetes.Interface, nodeNames []string, expectedHash string, bundleData map[string]string) error {
	if err := createAndWaitForCertSyncPods(ctx, kubeClient, nodeNames, bundleData); err != nil {
		klog.Warningf("Cert-sync pods failed: %v; falling back to local-only cert sync", err)
		if syncErr := waitForCertSync(ctx, expectedHash); syncErr != nil {
			klog.Warningf("Local cert sync timed out: %v; writing to local disk directly", syncErr)
			if writeErr := writeBundleToDisk(ctx, bundleData); writeErr != nil {
				return fmt.Errorf("all cert sync methods failed (pods: %v, wait: %v, write: %w)", err, syncErr, writeErr)
			}
		}
		klog.Warning("Only local node cert files verified; remote node cert sync is best-effort")
	}
	return nil
}

// createAndWaitForCertSyncPods creates a short-lived pod on each node that
// writes the CA bundle data to the host disk via a hostPath volume. The bundle
// data is embedded directly in the pod command (base64-encoded) so there is no
// dependency on ConfigMap volume caching or the API being available after pod
// creation. Must be called while the API is still reachable.
func createAndWaitForCertSyncPods(ctx context.Context, kubeClient kubernetes.Interface, nodeNames []string, bundleData map[string]string) error {
	image := os.Getenv("OPERATOR_IMAGE")
	if image == "" {
		return fmt.Errorf("OPERATOR_IMAGE not set")
	}

	script := buildCertWriteScript(bundleData)

	podNames := make([]string, 0, len(nodeNames))
	for _, nodeName := range nodeNames {
		podName := fmt.Sprintf("tnf-cert-sync-%s", nodeName)
		podNames = append(podNames, podName)

		// Clean up any leftover pod from a previous attempt
		if err := kubeClient.CoreV1().Pods(operatorclient.TargetNamespace).Delete(ctx, podName, metav1.DeleteOptions{}); err == nil {
			waitForPodDeletion(ctx, kubeClient, podName)
		}

		privileged := true
		hostPathType := corev1.HostPathDirectoryOrCreate
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: operatorclient.TargetNamespace,
				Labels:    map[string]string{"app": "tnf-cert-sync"},
				Annotations: map[string]string{
					"openshift.io/required-scc": "privileged",
				},
			},
			Spec: corev1.PodSpec{
				NodeName:                      nodeName,
				ServiceAccountName:            "tnf-setup-manager",
				PriorityClassName:             "system-node-critical",
				RestartPolicy:                 corev1.RestartPolicyNever,
				TerminationGracePeriodSeconds: int64Ptr(10),
				Tolerations: []corev1.Toleration{{
					Operator: corev1.TolerationOpExists,
				}},
				Containers: []corev1.Container{{
					Name:    "sync",
					Image:   image,
					Command: []string{"/bin/sh", "-c", script},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "target", MountPath: "/target"},
					},
					SecurityContext: &corev1.SecurityContext{
						Privileged: &privileged,
					},
				}},
				Volumes: []corev1.Volume{{
					Name: "target",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: caBundleDiskDir,
							Type: &hostPathType,
						},
					},
				}},
			},
		}

		if _, err := kubeClient.CoreV1().Pods(operatorclient.TargetNamespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			cleanupCertSyncPods(ctx, kubeClient, podNames)
			return fmt.Errorf("failed to create cert-sync pod on %s: %w", nodeName, err)
		}
		klog.Infof("Created cert-sync pod on %s", nodeName)
	}

	defer cleanupCertSyncPods(ctx, kubeClient, podNames)

	for _, podName := range podNames {
		if err := waitForPodCompletion(ctx, kubeClient, podName); err != nil {
			return fmt.Errorf("cert-sync pod %s: %w", podName, err)
		}
		klog.Infof("Cert-sync pod %s completed successfully", podName)
	}

	klog.Info("Cert files synced to all nodes via cert-sync pods")
	return nil
}

// buildCertWriteScript creates a shell script that writes each bundle file
// from base64-encoded data. The data is embedded in the script so the pod has
// no runtime dependency on the kube API or ConfigMap volumes.
func buildCertWriteScript(bundleData map[string]string) string {
	keys := make([]string, 0, len(bundleData))
	for k := range bundleData {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, filename := range keys {
		encoded := base64.StdEncoding.EncodeToString([]byte(bundleData[filename]))
		parts = append(parts, fmt.Sprintf("printf '%%s' '%s' | base64 -d > /target/%s", encoded, filename))
	}
	parts = append(parts, "chmod 0600 /target/*")
	return strings.Join(parts, " && ")
}

func waitForPodDeletion(ctx context.Context, kubeClient kubernetes.Interface, podName string) {
	_ = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := kubeClient.CoreV1().Pods(operatorclient.TargetNamespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return true, nil
		}
		return false, nil
	})
}

func waitForPodCompletion(ctx context.Context, kubeClient kubernetes.Interface, podName string) error {
	return wait.PollUntilContextTimeout(ctx, certSyncPodPollInterval, certSyncPodTimeout, true, func(ctx context.Context) (bool, error) {
		pod, err := kubeClient.CoreV1().Pods(operatorclient.TargetNamespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			klog.V(4).Infof("Failed to get cert-sync pod %s: %v", podName, err)
			return false, nil
		}
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			return true, nil
		case corev1.PodFailed:
			return false, fmt.Errorf("pod %s failed", podName)
		default:
			return false, nil
		}
	})
}

func cleanupCertSyncPods(ctx context.Context, kubeClient kubernetes.Interface, podNames []string) {
	for _, podName := range podNames {
		if err := kubeClient.CoreV1().Pods(operatorclient.TargetNamespace).Delete(ctx, podName, metav1.DeleteOptions{}); err != nil {
			klog.V(4).Infof("Failed to delete cert-sync pod %s: %v", podName, err)
		}
	}
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

// readBundleConfigMap reads the etcd-all-bundles ConfigMap and returns its data
// and a content hash. Must be called at job startup while the API is still
// reachable — after CA rotation breaks etcd TLS, the API becomes unavailable.
func readBundleConfigMap(ctx context.Context, kubeClient kubernetes.Interface) (map[string]string, string, error) {
	cm, err := kubeClient.CoreV1().ConfigMaps(operatorclient.TargetNamespace).Get(
		ctx, tlshelpers.EtcdAllBundlesConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("failed to get ConfigMap %s: %w", tlshelpers.EtcdAllBundlesConfigMapName, err)
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

	return cm.Data, fmt.Sprintf("%x", h.Sum(nil)), nil
}

// waitForCertSync polls until the CA bundle files on disk match the expected
// hash. Fallback path used when cert-sync pods fail.
func waitForCertSync(ctx context.Context, expectedHash string) error {
	klog.Infof("Waiting for CA bundle on disk to match ConfigMap (expected: %s)", expectedHash[:12])

	return wait.PollUntilContextTimeout(ctx, certSyncPollInterval, certSyncTimeout, true, func(ctx context.Context) (bool, error) {
		diskHash, err := getDiskBundleHash(ctx)
		if err != nil {
			klog.V(4).Infof("Failed to get disk bundle hash: %v", err)
			return false, nil
		}

		if expectedHash == diskHash {
			klog.Infof("CA bundle on disk matches ConfigMap (hash: %s)", expectedHash[:12])
			return true, nil
		}

		klog.V(4).Infof("CA bundle not yet synced (expected: %s, disk: %s)", expectedHash[:12], diskHash[:12])
		return false, nil
	})
}

// writeBundleToDisk writes the ConfigMap data directly to the CA bundle
// directory on the local node via nsenter. Last-resort fallback.
func writeBundleToDisk(ctx context.Context, data map[string]string) error {
	klog.Infof("Writing %d bundle files directly to %s", len(data), caBundleDiskDir)

	if _, _, err := exec.Execute(ctx, fmt.Sprintf("mkdir -p %s", caBundleDiskDir)); err != nil {
		return fmt.Errorf("failed to create bundle directory: %w", err)
	}

	for filename, content := range data {
		path := fmt.Sprintf("%s/%s", caBundleDiskDir, filename)
		encoded := base64.StdEncoding.EncodeToString([]byte(content))
		cmd := fmt.Sprintf("printf '%%s' '%s' | base64 -d > %s && chmod 0600 %s", encoded, path, path)
		if _, _, err := exec.Execute(ctx, cmd); err != nil {
			return fmt.Errorf("failed to write %s: %w", path, err)
		}
		klog.Infof("Wrote %s (%d bytes)", path, len(content))
	}
	return nil
}

func getDiskBundleHash(ctx context.Context) (string, error) {
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

func int64Ptr(i int64) *int64 { return &i }
