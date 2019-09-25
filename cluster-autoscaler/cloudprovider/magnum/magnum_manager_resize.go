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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud/openstack/containerinfra/v1/clusters"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud/openstack/orchestration/v1/stackresources"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud/openstack/orchestration/v1/stacks"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/klog"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

// magnumManagerResize implements the magnumManager interface.
type magnumManagerResize struct {
	clusterClient *gophercloud.ServiceClient
	heatClient    *gophercloud.ServiceClient

	clusterName string
	stackID     string
	stackName   string

	kubeMinionsStackName string
	kubeMinionsStackID   string

	lastDelete time.Time

	failedNodesDeleted map[string]bool
}

// createMagnumManagerResize creates an instance of magnumManagerResize.
func createMagnumManagerResize(clusterClient, heatClient *gophercloud.ServiceClient, opts config.AutoscalingOptions) (*magnumManagerResize, error) {
	manager := magnumManagerResize{
		clusterClient: clusterClient,
		heatClient:    heatClient,
		clusterName:   opts.ClusterName,
	}

	manager.failedNodesDeleted = make(map[string]bool)

	cluster, err := clusters.Get(manager.clusterClient, manager.clusterName).Extract()
	if err != nil {
		return nil, fmt.Errorf("unable to access cluster %q: %v", manager.clusterName, err)
	}

	manager.stackID = cluster.StackID
	manager.stackName, err = manager.getStackName()
	if err != nil {
		return nil, fmt.Errorf("could not store stack name on manager: %v", err)
	}

	manager.kubeMinionsStackName, manager.kubeMinionsStackID, err = manager.getKubeMinionsStack()
	if err != nil {
		return nil, fmt.Errorf("could not store kube minions stack name/ID on manager: %v", err)
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

// getNodes returns ProviderIDs for all nodes in the node group,
// used to find any nodes which are unregistered in kubernetes.
//
// Nodes which are in CREATE_IN_PROGRESS state are also returned,
// so that the autoscaler is aware of them. Nodes which fail to be
// created are returned as failed instances *after* the cluster resize
// has finished, so that the autoscaler will not try to delete them
// while the cluster is still scaling up.
//
// The statuses of the cluster, heat stack, and minion stacks are all
// checked to be sure that the cluster has finished resizing, since a
// failed resize will immediately put the cluster into the UPDATE_FAILED
// state even though some nodes may still be creating. In particular
// this is the case for scale ups which exceed the project quota.
func (mgr *magnumManagerResize) getNodes(nodegroup string) ([]cloudprovider.Instance, error) {
	var nodes []cloudprovider.Instance

	/*if time.Since(mgr.lastDelete) < time.Minute {
		return nil, fmt.Errorf("not long enough since last delete")
	}*/

	minionResourcesPages, err := stackresources.List(mgr.heatClient, mgr.kubeMinionsStackName, mgr.kubeMinionsStackID, nil).AllPages()
	if err != nil {
		return nil, fmt.Errorf("could not list minion resources: %v", err)
	}

	minionResources, err := stackresources.ExtractResources(minionResourcesPages)
	if err != nil {
		return nil, fmt.Errorf("could not extract minion resources: %v", err)
	}

	stack, err := stacks.Get(mgr.heatClient, mgr.kubeMinionsStackName, mgr.kubeMinionsStackID).Extract()
	if err != nil {
		return nil, fmt.Errorf("could not get kube_minions stack from heat: %v", err)
	}

	clusterStatus, err := mgr.getClusterStatus()
	if err != nil {
		return nil, fmt.Errorf("could not get cluster status: %v", err)
	}

	// Check if the cluster is in an "in progress" status
	clusterChanging := statusIsChanging(clusterStatus) || statusIsChanging(stack.Status)
	for _, minion := range minionResources {
		clusterChanging = clusterChanging || statusIsChanging(minion.Status)
	}

	klog.Infof("Cluster status is %q, cluster is changing = %v", clusterStatus, clusterChanging)

	// mapping from minion index to server ID e.g
	// "0": "4c30961a-6e2f-42be-be01-5270e1546a89"
	refsMap := make(map[string]string)
	for _, output := range stack.Outputs {
		if output["output_key"] == "refs_map" {
			refsMapOutput := output["output_value"].(map[string]interface{})
			for index, ID := range refsMapOutput {
				refsMap[index] = ID.(string)
			}
		}
	}

	klog.Infof("refsMap: %#v", refsMap)

	for _, minion := range minionResources {
		minion.Links = nil //debug
		//klog.Infof("Minion resource: %#v", minion)
		instance := cloudprovider.Instance{Id: minion.Name, Status: &cloudprovider.InstanceStatus{}}

		switch minion.Status {
		case "DELETE_COMPLETE":
			// Don't return this instance
			continue
		case "DELETE_IN_PROGRESS":
			instance.Status.State = cloudprovider.InstanceDeleting
		case "INIT_COMPLETE", "CREATE_IN_PROGRESS":
			instance.Status.State = cloudprovider.InstanceCreating
		case "UPDATE_IN_PROGRESS":
			instance.Status.State = cloudprovider.InstanceCreating
			if !minion.UpdatedTime.IsZero() {
				// New nodes do not have updated time set, pre-existing nodes do.
				instance.Status.State = cloudprovider.InstanceRunning
				if serverID, found := refsMap[minion.Name]; found && serverID != "kube-minion" {
					instance.Id = fmt.Sprintf("openstack:///%s", serverID)
				}
			}
		case "CREATE_FAILED", "UPDATE_FAILED":
			instance.Status.State = cloudprovider.InstanceCreating
			if _, found := mgr.failedNodesDeleted[minion.Name]; found {
				instance.Status.State = cloudprovider.InstanceDeleting
			}
			if clusterChanging {
				// Don't report that this instance has failed until the cluster has finished updating
				klog.Infof("Ignoring error of failed node %s until cluster update complete", minion.Name)
				break
			}

			errorClass := cloudprovider.OtherErrorClass

			// check if the error message is for exceeding the project quota
			if strings.Contains(strings.ToLower(minion.StatusReason), "quota") {
				errorClass = cloudprovider.OutOfResourcesErrorClass
			}
			instance.Status.ErrorInfo = &cloudprovider.InstanceErrorInfo{
				ErrorClass:   errorClass,
				ErrorMessage: minion.StatusReason,
			}
		case "CREATE_COMPLETE", "UPDATE_COMPLETE":
			if serverID, found := refsMap[minion.Name]; found && serverID != "kube-minion" {
				instance.Id = fmt.Sprintf("openstack:///%s", serverID)
			}
			instance.Status.State = cloudprovider.InstanceRunning
		default:
			klog.Infof("Ignoring minion %s in state %s", minion.Name, minion.Status)
			continue
		}

		nodes = append(nodes, instance)
	}

	m, _ := json.MarshalIndent(nodes, "", "\t")
	klog.Infof("Returning node instances:\n%s", string(m))
	klog.Infof("Cluster status is %q, cluster is changing = %v", clusterStatus, clusterChanging)
	return nodes, nil
}

// deleteNodes deletes nodes by passing a comma separated list of names or IPs
// of minions to remove to heat, and simultaneously sets the new number of minions on the stack.
// The magnum node_count is then set to the new value (does not cause any more nodes to be removed).
//
// TODO: The two step process is required until https://storyboard.openstack.org/#!/story/2005052
// is complete, which will allow resizing with specific nodes to be deleted as a single Magnum operation.
func (mgr *magnumManagerResize) deleteNodes(nodegroup string, nodes []NodeRef, updatedNodeCount int) error {
	//anyFake := false
	mgr.lastDelete = time.Now()
	var nodesToRemove []string
	for _, nodeRef := range nodes {
		if nodeRef.IsFake {
			//anyFake = true
			mgr.failedNodesDeleted[nodeRef.Name] = true
			klog.Infof("Deleting fake node %s", nodeRef.Name)
			nodesToRemove = append(nodesToRemove, nodeRef.Name)
			continue
		}
		klog.V(0).Infof("manager deleting node: %s", nodeRef.Name)
		nodesToRemove = append(nodesToRemove, nodeRef.SystemUUID)
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

	/*if anyFake && false {
		// Sleep to let the deletion status propagate through the heat stacks.
		// During scale up the CA checks cloudprovider.Nodes() every loop (default every 10 seconds)
		// and if it checks again and the failed node is still in CREATE_FAILED it will try to delete
		// it again, which causes a lot of problems.
		klog.Info("Sleeping to let heat stack changes propagate")
		time.Sleep(5 * time.Second)
	}*/

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

// refresh has nothing to do for resize manager
func (mgr *magnumManagerResize) refresh() error {
	return nil
}

// getStackName finds the name of a stack matching a given ID.
func (mgr *magnumManagerResize) getStackName() (string, error) {
	stack, err := stacks.Find(mgr.heatClient, mgr.stackID).Extract()
	if err != nil {
		return "", fmt.Errorf("could not find stack with ID %s: %v", mgr.stackID, err)
	}
	klog.V(0).Infof("For stack ID %s, stack name is %s", mgr.stackID, stack.Name)
	return stack.Name, nil
}

// getKubeMinionsStack finds the nested kube_minions stack belonging to the main cluster stack,
// and returns its name and ID.
func (mgr *magnumManagerResize) getKubeMinionsStack() (name string, ID string, err error) {
	minionsResource, err := stackresources.Get(mgr.heatClient, mgr.stackName, mgr.stackID, "kube_minions").Extract()
	if err != nil {
		return "", "", fmt.Errorf("could not get kube_minions stack resource: %v", err)
	}

	stack, err := stacks.Find(mgr.heatClient, minionsResource.PhysicalID).Extract()
	if err != nil {
		return "", "", fmt.Errorf("could not find stack matching resource ID in heat: %v", err)
	}

	klog.V(0).Infof("Found nested kube_minions stack: name %s, ID %s", stack.Name, minionsResource.PhysicalID)

	return stack.Name, minionsResource.PhysicalID, nil
}
