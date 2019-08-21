package magnum

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud"
	th "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/magnum/gophercloud/testhelper"
)

// microversion type tests

func TestMicroversionFromString(t *testing.T) {
	tests := []struct {
		from        string
		shouldParse bool
		expected    *microversion
	}{
		{"1.7", true, &microversion{1, 7}},
		{"1.10", true, &microversion{1, 10}},
		{"2.0", true, &microversion{2, 0}},
		{"2", false, nil},
		{"2.", false, nil},
		{".0", false, nil},
		{".", false, nil},
		{"", false, nil},
		{"1.7.1", false, nil},
	}

	for _, test := range tests {
		t.Run(test.from, func(t *testing.T) {
			mv, err := microversionFromString(test.from)
			if !test.shouldParse {
				assert.Error(t, err)
				return
			}
			assert.Equal(t, test.expected, mv)
		})
	}
}

func TestMicroversionSatisfies(t *testing.T) {
	type compare struct {
		other         *microversion
		shouldSatisfy bool
	}

	tests := []struct {
		version   *microversion
		compareTo []compare
	}{
		{&microversion{1, 7}, []compare{{&microversion{1, 8}, false}, {&microversion{1, 7}, true}, {&microversion{1, 6}, true}}},
		{&microversion{1, 7}, []compare{{&microversion{2, 8}, false}, {&microversion{2, 7}, false}, {&microversion{2, 6}, false}}},
		{&microversion{2, 7}, []compare{{&microversion{1, 8}, true}, {&microversion{1, 7}, true}, {&microversion{1, 6}, true}}},
		{&microversion{1, 7}, []compare{{microversionResize, true}}},
	}

	for _, test := range tests {
		for _, cmp := range test.compareTo {
			name := fmt.Sprintf("%v>=%v", test.version, cmp.other)
			t.Run(name, func(t *testing.T) {
				satisfies := test.version.satisfies(cmp.other)
				assert.Equal(t, cmp.shouldSatisfy, satisfies)
			})
		}
	}
}

// microversion API tests

var apiVersionListResponse = `{
   "versions":[
      {
         "status":"CURRENT",
         "min_version":"1.1",
         "max_version":"1.4",
         "id":"v1",
         "links":[
            {
               "href":"http://10.164.180.104:9511/v1/",
               "rel":"self"
            }
         ]
      }
   ],
   "name":"OpenStack Magnum API",
   "description":"Magnum is an OpenStack project which aims to provide container management."
}`

func createMicroversionTestServiceClient() *gophercloud.ServiceClient {
	return &gophercloud.ServiceClient{
		ProviderClient: &gophercloud.ProviderClient{TokenID: "cbc36478b0bd8e67e89469c7749d4127"},
		Endpoint:       th.Endpoint() + "/",
	}
}

func TestGetAvailableMicroversion(t *testing.T) {
	th.SetupHTTP()
	defer th.TeardownHTTP()

	th.Mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		fmt.Fprint(w, apiVersionListResponse)
	})

	sc := createMicroversionTestServiceClient()

	ver, err := getAvailableAPIVersion(sc)
	assert.NoError(t, err)
	assert.Equal(t, &microversion{1, 4}, ver)

}
