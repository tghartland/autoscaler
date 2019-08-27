package magnum

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"

	"gopkg.in/gcfg.v1"
	netutil "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud/openstack"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud/openstack/containerinfra/v1/clusters"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/version"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/klog"
)

func readConfig(configReader io.Reader) (*Config, error) {
	var cfg Config
	if configReader != nil {
		if err := gcfg.ReadInto(&cfg, configReader); err != nil {
			return nil, fmt.Errorf("couldn't read cloud config: %v", err)
		}
	}
	return &cfg, nil
}

// createProviderClient creates and authenticates a gophercloud provider client.
func createProviderClient(cfg *Config, opts config.AutoscalingOptions) (*gophercloud.ProviderClient, error) {
	if opts.ClusterName == "" {
		return nil, errors.New("the cluster-name parameter must be set")
	}

	authOpts := toAuthOptsExt(*cfg)

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

	return provider, nil
}

// createClusterClient creates a gophercloud service client for communicating with magnum.
//
// The cluster object is retrieved once to check that the cluster-name given in the command line
// options is the UUID of the cluster, and if it was the cluster name then it is replaced with the UUID
func createClusterClient(cfg *Config, provider *gophercloud.ProviderClient, opts config.AutoscalingOptions) (*gophercloud.ServiceClient, error) {
	clusterClient, err := openstack.NewContainerInfraV1(provider, gophercloud.EndpointOpts{Type: "container-infra", Name: "magnum", Region: cfg.Global.Region})
	if err != nil {
		return nil, fmt.Errorf("could not create container-infra client: %v", err)
	}
	return clusterClient, nil
}

func checkClusterUUID(provider *gophercloud.ProviderClient, clusterClient *gophercloud.ServiceClient, opts config.AutoscalingOptions) error {
	cluster, err := clusters.Get(clusterClient, opts.ClusterName).Extract()
	if err != nil {
		return fmt.Errorf("unable to access cluster %q: %v", opts.ClusterName, err)
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

	return nil
}

func createHeatClient(cfg *Config, provider *gophercloud.ProviderClient, opts config.AutoscalingOptions) (*gophercloud.ServiceClient, error) {
	heatClient, err := openstack.NewOrchestrationV1(provider, gophercloud.EndpointOpts{Type: "orchestration", Name: "heat", Region: cfg.Global.Region})
	if err != nil {
		return nil, fmt.Errorf("could not create orchestration client: %v", err)
	}

	return heatClient, nil
}
