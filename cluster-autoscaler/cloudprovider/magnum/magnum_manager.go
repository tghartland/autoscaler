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
	"io"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

// magnumManager is an interface for the basic interactions with the cluster.
type magnumManager interface {
	nodeGroupSize(nodegroup string) (int, error)
	updateNodeCount(nodegroup string, nodes int) error
	getNodes(nodegroup string) ([]string, error)
	deleteNodes(nodegroup string, nodes []NodeRef, updatedNodeCount int) error
	getClusterStatus() (string, error)
	canUpdate() (bool, string, error)
	templateNodeInfo(nodegroup string) (*schedulernodeinfo.NodeInfo, error)
}

// createMagnumManager creates the desired implementation of magnumManager.
// Currently reads the environment variable MAGNUM_MANAGER to find which to create,
// and falls back to a default if the variable is not found.
func createMagnumManager(configReader io.Reader, discoverOpts cloudprovider.NodeGroupDiscoveryOptions, opts config.AutoscalingOptions) (magnumManager, error) {
	return createMagnumManagerResize(configReader, discoverOpts, opts)
}
