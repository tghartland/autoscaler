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
	"errors"
	"fmt"
	"io"
	"net/http"

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

// createClusterClient creates the gophercloud service client for communicating with magnum.
//
// The cluster object is retrieved once to check that the cluster-name given in the command line
// options is the UUID of the cluster, and if it was the cluster name then it is replaced with the UUID
func createClusterClient(configReader io.Reader, opts config.AutoscalingOptions) (*gophercloud.ServiceClient, error) {
	var cfg Config
	if configReader != nil {
		if err := gcfg.ReadInto(&cfg, configReader); err != nil {
			return nil, fmt.Errorf("couldn't read cloud config: %v", err)
		}
	}

	if opts.ClusterName == "" {
		return nil, errors.New("the cluster-name parameter must be set")
	}

	authOpts := toAuthOptsExt(cfg)

	provider, err := openstack.NewClient(cfg.Global.AuthURL)
	if err != nil {
		return nil, fmt.Errorf("could not create openstack client: %v", err)
	}

	userAgent := gophercloud.UserAgent{}
	userAgent.Prepend(fmt.Sprintf("cluster-autoscaler/%s", version.ClusterAutoscalerVersion))
	userAgent.Prepend(fmt.Sprintf("cluster/%s", opts.ClusterName))
	provider.UserAgent = userAgent

	klog.V(5).Infof("Using user-agent %q", userAgent.Join())

	if cfg.Global.CAFile != "" {
		roots, err := certutil.NewPool(cfg.Global.CAFile)
		if err != nil {
			return nil, err
		}
		config := &tls.Config{}
		config.RootCAs = roots
		provider.HTTPClient.Transport = netutil.SetOldTransportDefaults(&http.Transport{TLSClientConfig: config})
	}

	err = openstack.AuthenticateV3(provider, authOpts, gophercloud.EndpointOpts{})
	if err != nil {
		return nil, fmt.Errorf("could not authenticate client: %v", err)
	}

	clusterClient, err := openstack.NewContainerInfraV1(provider, gophercloud.EndpointOpts{Type: "container-infra", Name: "magnum", Region: cfg.Global.Region})
	if err != nil {
		return nil, fmt.Errorf("could not create container-infra client: %v", err)
	}

	cluster, err := clusters.Get(clusterClient, opts.ClusterName).Extract()
	if err != nil {
		return nil, fmt.Errorf("unable to access cluster %q: %v", opts.ClusterName, err)
	}

	// Prefer to use the cluster UUID if the cluster name was given in the parameters
	if opts.ClusterName != cluster.UUID {
		klog.V(2).Infof("Using cluster UUID %q instead of name %q", cluster.UUID, opts.ClusterName)
		opts.ClusterName = cluster.UUID

		// Need to remake user-agent with UUID instead of name
		userAgent := gophercloud.UserAgent{}
		userAgent.Prepend(fmt.Sprintf("cluster-autoscaler/%s", version.ClusterAutoscalerVersion))
		userAgent.Prepend(fmt.Sprintf("cluster/%s", opts.ClusterName))
		provider.UserAgent = userAgent
		klog.V(5).Infof("Using updated user-agent %q", userAgent.Join())
	}

	return clusterClient, nil
}

// createMagnumManager creates the correct implementation of magnumManager for the available API version.
func createMagnumManager(configReader io.Reader, discoverOpts cloudprovider.NodeGroupDiscoveryOptions, opts config.AutoscalingOptions) (magnumManager, error) {
	clusterClient, err := createClusterClient(configReader, opts)
	if err != nil {
		return nil, fmt.Errorf("could not create client: %v", err)
	}

	apiVersion, err := getAvailableAPIVersion(clusterClient)
	if err != nil {
		return nil, fmt.Errorf("could not check available magnum API microversion: %v", err)
	}

	clusterClient.Microversion = apiVersion.String()

	switch {
	case apiVersion.satisfies(microversionResize):
		return createMagnumManagerResize(clusterClient, opts)
	default:
		return nil, fmt.Errorf("no magnum manager available for API microversion %q", apiVersion)
	}
}
