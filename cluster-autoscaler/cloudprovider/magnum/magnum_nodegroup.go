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
	"fmt"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/klog"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

// magnumNodeGroup implements NodeGroup interface from cluster-autoscaler/cloudprovider.
//
// Represents a homogeneous collection of nodes within a cluster,
// which can be dynamically resized between a minimum and maximum
// number of nodes.
type magnumNodeGroup struct {
	magnumManager magnumManager
	id            string

	clusterUpdateMutex *sync.Mutex

	minSize int
	maxSize int
	// Stored as a pointer so that when autoscaler copies the nodegroup it can still update the target size
	targetSize *int

	nodesToDelete      []*apiv1.Node
	nodesToDeleteMutex sync.Mutex

	waitTimeStep        time.Duration
	deleteBatchingDelay time.Duration

	// Used so that only one DeleteNodes goroutine has to get the node group size at the start of the deletion
	deleteNodesCachedSize   int
	deleteNodesCachedSizeAt time.Time
}

// waitForClusterStatus checks periodically to see if the cluster has entered a given status.
// Returns when the status is observed or the timeout is reached.
func (ng *magnumNodeGroup) waitForClusterStatus(status string, timeout time.Duration) error {
	klog.V(2).Infof("Waiting for cluster %s status", status)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(ng.waitTimeStep) {
		clusterStatus, err := ng.magnumManager.getClusterStatus()
		if err != nil {
			return fmt.Errorf("error waiting for %s status: %v", status, err)
		}
		if clusterStatus == status {
			klog.V(0).Infof("Waited for cluster %s status", status)
			return nil
		}
	}
	return fmt.Errorf("timeout (%v) waiting for %s status", timeout, status)
}

// IncreaseSize increases the number of nodes by replacing the cluster's node_count.
//
// Takes precautions so that the cluster is not modified while in an UPDATE_IN_PROGRESS state.
// Blocks until the cluster has reached UPDATE_COMPLETE.
func (ng *magnumNodeGroup) IncreaseSize(delta int) error {
	ng.clusterUpdateMutex.Lock()
	defer ng.clusterUpdateMutex.Unlock()

	if delta <= 0 {
		return fmt.Errorf("size increase must be positive")
	}

	size := *ng.targetSize
	if size+delta > ng.MaxSize() {
		return fmt.Errorf("size increase too large, desired:%d max:%d", size+delta, ng.MaxSize())
	}

	updatePossible, currentStatus, err := ng.magnumManager.canUpdate()
	if err != nil {
		return fmt.Errorf("can not increase node count: %v", err)
	}
	if !updatePossible {
		return fmt.Errorf("can not add nodes, cluster is in %s status", currentStatus)
	}
	klog.V(0).Infof("Increasing size by %d, %d->%d", delta, *ng.targetSize, *ng.targetSize+delta)
	*ng.targetSize += delta

	err = ng.magnumManager.updateNodeCount(ng.id, *ng.targetSize)
	if err != nil {
		return fmt.Errorf("could not increase cluster size: %v", err)
	}

	klog.V(0).Info("Returning from IncreaseSize")
	return nil
}

// deleteNodes deletes a set of nodes chosen by the autoscaler.
//
// The process of deletion depends on the implementation of magnumManager,
// but this function handles what should be common between all implementations:
//   - simultaneous but separate calls from the autoscaler are batched together
//   - does not allow scaling while the cluster is already in an UPDATE_IN_PROGRESS state
//   - after scaling down, blocks until the cluster has reached UPDATE_COMPLETE
func (ng *magnumNodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	ng.clusterUpdateMutex.Lock()
	defer ng.clusterUpdateMutex.Unlock()
	cachedSize := *ng.targetSize

	var nodeNames []string
	for _, node := range nodes {
		nodeNames = append(nodeNames, node.Name)
	}
	klog.V(1).Infof("Deleting nodes: %v", nodeNames)

	updatePossible, currentStatus, err := ng.magnumManager.canUpdate()
	if err != nil {
		return fmt.Errorf("could not check if cluster is ready to delete nodes: %v", err)
	}
	if !updatePossible {
		return fmt.Errorf("can not delete nodes, cluster is in %s status", currentStatus)
	}

	// Double check that the total number of batched nodes for deletion will not take the node group below its minimum size
	if cachedSize-len(nodes) < ng.MinSize() {
		return fmt.Errorf("size decrease too large, desired:%d min:%d", cachedSize-len(nodes), ng.MinSize())
	}

	var nodeRefs []NodeRef
	for _, node := range nodes {

		// Find node IPs, can be multiple (IPv4 and IPv6)
		var IPs []string
		for _, addr := range node.Status.Addresses {
			if addr.Type == apiv1.NodeInternalIP {
				IPs = append(IPs, addr.Address)
			}
		}
		nodeRefs = append(nodeRefs, NodeRef{
			Name:       node.Name,
			MachineID:  node.Status.NodeInfo.MachineID,
			ProviderID: node.Spec.ProviderID,
			IPs:        IPs,
		})
	}

	err = ng.magnumManager.deleteNodes(ng.id, nodeRefs, cachedSize-len(nodes))
	if err != nil {
		return fmt.Errorf("manager error deleting nodes: %v", err)
	}

	*ng.targetSize = cachedSize - len(nodes)

	klog.V(0).Info("Returning from DeleteNodes")
	return nil
}

// DecreaseTargetSize decreases the cluster node_count in magnum.
func (ng *magnumNodeGroup) DecreaseTargetSize(delta int) error {
	if delta >= 0 {
		return fmt.Errorf("size decrease must be negative")
	}
	klog.V(0).Infof("Decreasing target size by %d, %d->%d", delta, *ng.targetSize, *ng.targetSize+delta)
	*ng.targetSize += delta
	return ng.magnumManager.updateNodeCount(ng.id, *ng.targetSize)
}

// Id returns the node group ID
func (ng *magnumNodeGroup) Id() string {
	return ng.id
}

// Debug returns a string formatted with the node group's min, max and target sizes.
func (ng *magnumNodeGroup) Debug() string {
	return fmt.Sprintf("%s min=%d max=%d target=%d", ng.id, ng.minSize, ng.maxSize, *ng.targetSize)
}

// Nodes returns a list of nodes that belong to this node group.
func (ng *magnumNodeGroup) Nodes() ([]cloudprovider.Instance, error) {
	nodes, err := ng.magnumManager.getNodes(ng.id)
	if err != nil {
		return nil, fmt.Errorf("could not get nodes: %v", err)
	}
	var instances []cloudprovider.Instance
	for _, node := range nodes {
		instances = append(instances, cloudprovider.Instance{Id: node})
	}
	return instances, nil
}

// TemplateNodeInfo returns a node template for this node group.
func (ng *magnumNodeGroup) TemplateNodeInfo() (*schedulernodeinfo.NodeInfo, error) {
	return ng.magnumManager.templateNodeInfo(ng.id)
}

// Exist returns if this node group exists.
// Currently always returns true.
func (ng *magnumNodeGroup) Exist() bool {
	return true
}

// Create creates the node group on the cloud provider side.
func (ng *magnumNodeGroup) Create() (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrAlreadyExist
}

// Delete deletes the node group on the cloud provider side.
func (ng *magnumNodeGroup) Delete() error {
	return cloudprovider.ErrNotImplemented
}

// Autoprovisioned returns if the nodegroup is autoprovisioned.
func (ng *magnumNodeGroup) Autoprovisioned() bool {
	return false
}

// MaxSize returns the maximum allowed size of the node group.
func (ng *magnumNodeGroup) MaxSize() int {
	return ng.maxSize
}

// MinSize returns the minimum allowed size of the node group.
func (ng *magnumNodeGroup) MinSize() int {
	return ng.minSize
}

// TargetSize returns the target size of the node group.
func (ng *magnumNodeGroup) TargetSize() (int, error) {
	return *ng.targetSize, nil
}
