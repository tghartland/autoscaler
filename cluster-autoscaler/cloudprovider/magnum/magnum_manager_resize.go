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
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/satori/go.uuid"
	"gopkg.in/gcfg.v1"
	netutil "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud/openstack"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud/openstack/containerinfra/v1/clusters"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/version"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/klog"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

// magnumManagerResize implements the magnumManager interface.
//
// Most interactions with the cluster are done directly with magnum,
// but scaling down requires an intermediate step using heat to
// delete the specific nodes that the autoscaler has picked for removal.
type magnumManagerResize struct {
	clusterClient *gophercloud.ServiceClient
	clusterName   string

	waitTimeStep time.Duration
}

// createMagnumManagerHeat sets up cluster and stack clients and returns
// an magnumManagerResize.
func createMagnumManagerResize(configReader io.Reader, discoverOpts cloudprovider.NodeGroupDiscoveryOptions, opts config.AutoscalingOptions) (*magnumManagerResize, error) {
	var cfg Config
	if configReader != nil {
		if err := gcfg.ReadInto(&cfg, configReader); err != nil {
			klog.Errorf("Couldn't read config: %v", err)
			return nil, err
		}
	}

	if opts.ClusterName == "" {
		klog.Fatalf("The cluster-name parameter must be set")
	}

	authOpts := toAuthOptsExt(cfg)

	provider, err := openstack.NewClient(cfg.Global.AuthURL)
	if err != nil {
		return nil, fmt.Errorf("could not authenticate client: %v", err)
	}

	if cfg.Global.CAFile != "" {
		roots, err := certutil.NewPool(cfg.Global.CAFile)
		if err != nil {
			return nil, err
		}
		config := &tls.Config{}
		config.RootCAs = roots
		provider.HTTPClient.Transport = netutil.SetOldTransportDefaults(&http.Transport{TLSClientConfig: config})

	}

	userAgent := gophercloud.UserAgent{}
	userAgent.Prepend(fmt.Sprintf("cluster-autoscaler/%s", version.ClusterAutoscalerVersion))
	userAgent.Prepend(fmt.Sprintf("cluster/%s", opts.ClusterName))
	provider.UserAgent = userAgent

	klog.V(5).Infof("Using user-agent %s", userAgent.Join())

	err = openstack.AuthenticateV3(provider, authOpts, gophercloud.EndpointOpts{})
	if err != nil {
		return nil, fmt.Errorf("could not authenticate: %v", err)
	}

	clusterClient, err := openstack.NewContainerInfraV1(provider, gophercloud.EndpointOpts{Type: "container-infra", Name: "magnum", Region: cfg.Global.Region})
	if err != nil {
		return nil, fmt.Errorf("could not create container-infra client: %v", err)
	}
	clusterClient.Microversion = "1.7"

	manager := magnumManagerResize{
		clusterClient: clusterClient,
		clusterName:   opts.ClusterName,
		waitTimeStep:  waitForStatusTimeStep,
	}

	cluster, err := clusters.Get(manager.clusterClient, manager.clusterName).Extract()
	if err != nil {
		return nil, fmt.Errorf("unable to access cluster (%s): %v", manager.clusterName, err)
	}

	// Prefer to use the cluster UUID if the cluster name was given in the parameters
	if cluster.UUID != opts.ClusterName {
		klog.V(0).Infof("Using cluster UUID %s instead of name %s", cluster.UUID, opts.ClusterName)
		manager.clusterName = cluster.UUID

		userAgent := gophercloud.UserAgent{}
		userAgent.Prepend(fmt.Sprintf("cluster-autoscaler/%s", version.ClusterAutoscalerVersion))
		userAgent.Prepend(fmt.Sprintf("cluster/%s", cluster.UUID))
		provider.UserAgent = userAgent

		klog.V(5).Infof("Using updated user-agent %s", userAgent.Join())
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
	clusterStatus, err := mgr.getClusterStatus()
	if err != nil {
		return false, "", fmt.Errorf("could not get cluster status: %v", err)
	}
	return !statusesPreventingUpdate.Has(clusterStatus), clusterStatus, nil
}

// templateNodeInfo returns a NodeInfo with a node template based on the VM flavor
// that is used to created minions in a given node group.
func (mgr *magnumManagerResize) templateNodeInfo(nodegroup string) (*schedulernodeinfo.NodeInfo, error) {
	// TODO: create a node template by getting the minion flavor from the heat stack.
	return nil, cloudprovider.ErrNotImplemented
}
