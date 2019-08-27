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
	"io"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

// magnumManager is an interface for the basic interactions with the cluster.
type magnumManager interface {
	nodeGroupSize(nodegroup string) (int, error)
	updateNodeCount(nodegroup string, nodes int) error
	getNodes(nodegroup string) ([]cloudprovider.Instance, error)
	deleteNodes(nodegroup string, nodes []NodeRef, updatedNodeCount int) error
	getClusterStatus() (string, error)
	canUpdate() (bool, string, error)
	templateNodeInfo(nodegroup string) (*schedulernodeinfo.NodeInfo, error)
	refresh() error
}

// createMagnumManager creates the correct implementation of magnumManager for the available API version.
func createMagnumManager(configReader io.Reader, discoverOpts cloudprovider.NodeGroupDiscoveryOptions, opts config.AutoscalingOptions) (magnumManager, error) {
	cfg, err := readConfig(configReader)
	if err != nil {
		return nil, err
	}

	provider, err := createProviderClient(cfg, opts)
	if err != nil {
		return nil, fmt.Errorf("could not create provider client: %v", err)
	}

	clusterClient, err := createClusterClient(cfg, provider, opts)
	if err != nil {
		return nil, err
	}

	err = checkClusterUUID(provider, clusterClient, opts)
	if err != nil {
		return nil, fmt.Errorf("could not check cluster UUID: %v", err)
	}

	apiVersion, err := getAvailableAPIVersion(clusterClient)
	if err != nil {
		return nil, fmt.Errorf("could not check available magnum API microversion: %v", err)
	}

	clusterClient.Microversion = apiVersion.String()

	switch {
	case apiVersion.satisfies(microversionResize):
		heatClient, err := createHeatClient(cfg, provider, opts)
		if err != nil {
			return nil, err
		}
		return createMagnumManagerResize(clusterClient, heatClient, opts)
	default:
		return nil, fmt.Errorf("no magnum manager available for API microversion %q", apiVersion)
	}
}
