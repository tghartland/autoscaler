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

	"github.com/satori/go.uuid"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud/openstack/containerinfra/v1/clusters"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/klog"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

// magnumManagerResize implements the magnumManager interface.
type magnumManagerResize struct {
	clusterClient *gophercloud.ServiceClient
	clusterName   string
}

// createMagnumManagerResize creates an instance of magnumManagerResize.
func createMagnumManagerResize(clusterClient *gophercloud.ServiceClient, opts config.AutoscalingOptions) (*magnumManagerResize, error) {
	manager := magnumManagerResize{
		clusterClient: clusterClient,
		clusterName:   opts.ClusterName,
	}

	return &manager, nil
}

// nodeGroupSize gets the current cluster size as reported by magnum.
// The nodegroup argument is ignored as this implementation of magnumManager
// assumes that only a single node group exists.
func (mgr *magnumManagerResize) nodeGroupSize(nodegroup string) (int, error) {
	cluster, err := clusters.Get(mgr.clusterClient, mgr.clusterName).Extract()
	if err != nil {
		return 0, fmt.Errorf("could not get cluster: %v", err)
	}
	return cluster.NodeCount, nil
}

// updateNodeCount replaces the cluster node_count in magnum.
func (mgr *magnumManagerResize) updateNodeCount(nodegroup string, nodes int) error {
	resizeOpts := clusters.ResizeOpts{
		NodeCount: &nodes,
		NodeGroup: nodegroup,
	}

	resizeResult := clusters.Resize(mgr.clusterClient, mgr.clusterName, resizeOpts)
	err := resizeResult.Extract()
	if err != nil {
		return fmt.Errorf("could not resize cluster: %v", err)
	}
	return nil
}

// getNodes should return ProviderIDs for all nodes in the node group,
// used to find any nodes which are unregistered in kubernetes.
// This can not be done with heat currently but a change has been merged upstream
// that will allow this.
func (mgr *magnumManagerResize) getNodes(nodegroup string) ([]string, error) {
	// TODO: get node ProviderIDs by getting nova instance IDs from heat
	// Waiting for https://review.openstack.org/#/c/639053/ to be able to get
	// nova instance IDs from the kube_minions stack resource.
	// This works fine being empty for now anyway.
	return []string{}, nil
}

// deleteNodes deletes nodes by passing a comma separated list of names or IPs
// of minions to remove to heat, and simultaneously sets the new number of minions on the stack.
// The magnum node_count is then set to the new value (does not cause any more nodes to be removed).
//
// TODO: The two step process is required until https://storyboard.openstack.org/#!/story/2005052
// is complete, which will allow resizing with specific nodes to be deleted as a single Magnum operation.
func (mgr *magnumManagerResize) deleteNodes(nodegroup string, nodes []NodeRef, updatedNodeCount int) error {
	var nodesToRemove []string
	for _, nodeRef := range nodes {
		klog.V(0).Infof("manager deleting node: %s", nodeRef.Name)
		id, err := uuid.FromString(nodeRef.MachineID)
		if err != nil {
			return fmt.Errorf("could not convert node machine ID to openstack format: %v", err)
		}
		openstackFormatID := id.String()
		nodesToRemove = append(nodesToRemove, openstackFormatID)
	}

	resizeOpts := clusters.ResizeOpts{
		NodeCount:     &updatedNodeCount,
		NodesToRemove: nodesToRemove,
		NodeGroup:     nodegroup,
	}

	klog.V(0).Infof("resizeOpts: node_count=%d, remove=%v", *resizeOpts.NodeCount, resizeOpts.NodesToRemove)

	resizeResult := clusters.Resize(mgr.clusterClient, mgr.clusterName, resizeOpts)
	err := resizeResult.Extract()
	if err != nil {
		return fmt.Errorf("could not resize cluster: %v", err)
	}

	return nil
}

// getClusterStatus returns the current status of the magnum cluster.
func (mgr *magnumManagerResize) getClusterStatus() (string, error) {
	cluster, err := clusters.Get(mgr.clusterClient, mgr.clusterName).Extract()
	if err != nil {
		return "", fmt.Errorf("could not get cluster: %v", err)
	}
	return cluster.Status, nil
}

// canUpdate checks if the cluster status is present in a set of statuses that
// prevent the cluster from being updated.
// Returns if updating is possible and the status for convenience.
func (mgr *magnumManagerResize) canUpdate() (bool, string, error) {
	return true, "", nil
}

// templateNodeInfo returns a NodeInfo with a node template based on the VM flavor
// that is used to created minions in a given node group.
func (mgr *magnumManagerResize) templateNodeInfo(nodegroup string) (*schedulernodeinfo.NodeInfo, error) {
	// TODO: create a node template by getting the minion flavor from the heat stack.
	return nil, cloudprovider.ErrNotImplemented
}

// refresh not implemented
func (mgr *magnumManagerResize) resfresh() error {
	return nil
}
