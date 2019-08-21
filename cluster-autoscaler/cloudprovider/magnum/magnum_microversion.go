package magnum

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud/openstack/containerinfra/apiversions"
)

type microversion struct {
	major int
	minor int
}

func (m *microversion) satisfies(other *microversion) bool {
	if m.major > other.major {
		return true
	}
	if m.major == other.major && m.minor >= other.minor {
		return true
	}
	return false
}

func (m *microversion) String() string {
	return fmt.Sprintf("%d.%d", m.major, m.minor)
}

func microversionFromString(version string) (*microversion, error) {
	parts := strings.Split(version, ".")
	if len(parts) != 2 {
		return nil, errors.New("could not split into major and minor part")

	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("could not parse major part as int: %v", err)
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("could not parse minor part as int: %v", err)
	}

	return &microversion{major, minor}, nil
}

var (
	microversionResize = &microversion{1, 7}
)

// getVersion finds a given apiversions.APIVersion in from a list, by major version
func getVersion(versions []apiversions.APIVersion, version string) (apiversions.APIVersion, error) {
	for _, v := range versions {
		if v.ID == version {
			return v, nil
		}
	}
	return apiversions.APIVersion{}, fmt.Errorf("version %q not found", version)
}

// getAvailableAPIVersion gets the available magnum microversion (e.g "1.7")
func getAvailableAPIVersion(clusterClient *gophercloud.ServiceClient) (*microversion, error) {
	verPages, err := apiversions.List(clusterClient).AllPages()
	if err != nil {
		return nil, fmt.Errorf("could not list magnum API versions: %v", err)
	}

	versions, err := apiversions.ExtractAPIVersions(verPages)
	if err != nil {
		return nil, fmt.Errorf("could not extract magnum API versions: %v", err)
	}

	v1, err := getVersion(versions, "v1")
	if err != nil {
		return nil, err
	}

	v1Microversion, err := microversionFromString(v1.Version)
	if err != nil {
		return nil, fmt.Errorf("could not parse microversion %q: %v", v1.Version, err)
	}

	return v1Microversion, nil
}
