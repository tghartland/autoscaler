/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package magnum

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

type magnumManagerMock struct {
	mock.Mock
}

func (m *magnumManagerMock) nodeGroupSize(nodegroup string) (int, error) {
	args := m.Called(nodegroup)
	return args.Int(0), args.Error(1)
}

func (m *magnumManagerMock) updateNodeCount(nodegroup string, nodes int) error {
	args := m.Called(nodegroup, nodes)
	return args.Error(0)
}

func (m *magnumManagerMock) getNodes(nodegroup string) ([]string, error) {
	args := m.Called(nodegroup)
	return args.Get(0).([]string), args.Error(1)
}

func (m *magnumManagerMock) deleteNodes(nodegroup string, nodes []NodeRef, updatedNodeCount int) error {
	args := m.Called(nodegroup, nodes, updatedNodeCount)
	return args.Error(0)
}

func (m *magnumManagerMock) getClusterStatus() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func (m *magnumManagerMock) canUpdate() (bool, string, error) {
	args := m.Called()
	return args.Bool(0), args.String(1), args.Error(2)
}

func (m *magnumManagerMock) templateNodeInfo(nodegroup string) (*schedulernodeinfo.NodeInfo, error) {
	return &schedulernodeinfo.NodeInfo{}, nil
}

func createTestNodeGroup(manager magnumManager) *magnumNodeGroup {
	current := 1
	ng := magnumNodeGroup{
		magnumManager:      manager,
		id:                 "TestNodeGroup",
		clusterUpdateMutex: &sync.Mutex{},
		minSize:            1,
		maxSize:            10,
		targetSize:         &current,
	}
	return &ng
}

func TestIncreaseSize(t *testing.T) {
	manager := &magnumManagerMock{}
	ng := createTestNodeGroup(manager)

	// Test all working normally
	t.Run("success", func(t *testing.T) {
		manager.On("updateNodeCount", "TestNodeGroup", 2).Return(nil).Once()
		err := ng.IncreaseSize(1)
		assert.NoError(t, err)
		assert.Equal(t, 2, *ng.targetSize, "target size not updated")
	})

	// Test negative increase
	t.Run("negative increase", func(t *testing.T) {
		err := ng.IncreaseSize(-1)
		assert.Error(t, err)
		assert.Equal(t, "size increase must be positive", err.Error())
	})

	// Test zero increase
	t.Run("zero increase", func(t *testing.T) {
		err := ng.IncreaseSize(0)
		assert.Error(t, err)
		assert.Equal(t, "size increase must be positive", err.Error())
	})

	// Test increase too large
	t.Run("increase too large", func(t *testing.T) {
		*ng.targetSize = 1
		manager.On("nodeGroupSize", "TestNodeGroup").Return(1, nil).Once()
		err := ng.IncreaseSize(10)
		assert.Error(t, err)
		assert.Equal(t, "size increase too large, desired:11 max:10", err.Error())
	})

	// Test update node count fails
	t.Run("update node count fails", func(t *testing.T) {
		*ng.targetSize = 1
		manager.On("updateNodeCount", "TestNodeGroup", 2).Return(errors.New("manager error")).Once()
		err := ng.IncreaseSize(1)
		assert.Error(t, err)
		assert.Equal(t, "could not increase cluster size: manager error", err.Error())
	})
}

var machineIDs []string

var nodesToDelete []*apiv1.Node
var nodeRefs []NodeRef

func init() {
	machineIDs = []string{
		"4d030dc5f2944154aaba43a004f9fccc",
		"ce120bf512f14497be3c1b07ac9a433a",
		"7cce782693dd4805bc8e2735d3600433",
		"8bf92ce193f7401c9a24d1dc2e75dc5d",
		"1ef6ab6407c3482185fbff428271a6a0",
	}
	for i, machineID := range machineIDs {
		nodeRefs = append(nodeRefs,
			NodeRef{Name: fmt.Sprintf("cluster-abc-minion-%d", i+1),
				MachineID:  machineID,
				ProviderID: fmt.Sprintf("openstack:///%s", machineID),
				IPs:        []string{fmt.Sprintf("10.0.0.%d", 100+i)},
			},
		)
	}

	for i := 0; i < 5; i++ {
		node := apiv1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("cluster-abc-minion-%d", i+1),
			},
			Status: apiv1.NodeStatus{
				NodeInfo: apiv1.NodeSystemInfo{
					MachineID: machineIDs[i],
				},
				Addresses: []apiv1.NodeAddress{
					{Type: apiv1.NodeInternalIP, Address: fmt.Sprintf("10.0.0.%d", 100+i)},
				},
			},
			Spec: apiv1.NodeSpec{
				ProviderID: fmt.Sprintf("openstack:///%s", machineIDs[i]),
			},
		}

		nodesToDelete = append(nodesToDelete, &node)
	}
}

func TestDeleteNodes(t *testing.T) {
	manager := &magnumManagerMock{}
	ng := createTestNodeGroup(manager)

	// Test all working normally
	t.Run("success", func(t *testing.T) {
		*ng.targetSize = 10
		manager.On("deleteNodes", "TestNodeGroup", nodeRefs, 5).Return(nil).Once()
		err := ng.DeleteNodes(nodesToDelete)
		assert.NoError(t, err)
		assert.Equal(t, 5, *ng.targetSize)
	})

	// Test call to deleteNodes on manager failing
	t.Run("deleteNodes fails", func(t *testing.T) {
		*ng.targetSize = 10
		manager.On("deleteNodes", "TestNodeGroup", nodeRefs, 5).Return(errors.New("manager error")).Once()
		err := ng.DeleteNodes(nodesToDelete)
		assert.Error(t, err)
		assert.Equal(t, "manager error deleting nodes: manager error", err.Error())
	})
}
