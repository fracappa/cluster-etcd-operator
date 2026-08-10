package etcdrestart

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGetControlPlaneNodeNames(t *testing.T) {
	tests := []struct {
		name        string
		nodes       []*corev1.Node
		expectNames []string
		expectError bool
	}{
		{
			name: "two control plane nodes returned sorted",
			nodes: []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "master-1", Labels: map[string]string{"node-role.kubernetes.io/master": ""}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "master-0", Labels: map[string]string{"node-role.kubernetes.io/master": ""}}},
			},
			expectNames: []string{"master-0", "master-1"},
			expectError: false,
		},
		{
			name: "only master-labeled nodes returned",
			nodes: []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "master-0", Labels: map[string]string{"node-role.kubernetes.io/master": ""}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Labels: map[string]string{"node-role.kubernetes.io/worker": ""}}},
			},
			expectNames: []string{"master-0"},
			expectError: false,
		},
		{
			name:        "no nodes returns error",
			nodes:       []*corev1.Node{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientset()
			ctx := context.Background()

			for _, node := range tt.nodes {
				_, err := fakeClient.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
				require.NoError(t, err)
			}

			names, err := getControlPlaneNodeNames(ctx, fakeClient)
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expectNames, names)
		})
	}
}
