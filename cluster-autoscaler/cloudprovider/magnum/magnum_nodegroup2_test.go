package magnum

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func testDeleteNodes2(t *testing.T) {
	manager := &magnumManagerMock{}
	ng := createTestNodeGroup(manager)
	*ng.targetSize = 1000000
	ng.maxSize = 1000000

	// Test all working normally
	t.Run("success", func(t *testing.T) {
		var wg sync.WaitGroup
		test := func(ng *magnumNodeGroup) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				manager.On("canUpdate").Return(true, clusterStatusUpdateComplete, nil)
				manager.On("deleteNodes", "TestNodeGroup", mock.Anything, mock.Anything).Return(nil)
				err := ng.DeleteNodes(nodesToDelete[:1])
				assert.NoError(t, err)
			}
		}
		for p := 0; p < 50; p++ {
			wg.Add(1)
			go test(ng)
		}
		wg.Wait()
		assert.Equal(t, *ng.targetSize, 1000000-(500*50))
	})
}

func TestDeleteNodesConcurrent(t *testing.T) {
	manager := &magnumManagerMock{}
	ng := createTestNodeGroup(manager)

	manager.On("deleteNodes", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	initialSize := 3000
	threads := 20
	deletesPerThread := 100
	expectedFinalSize := initialSize - (threads * deletesPerThread)

	*ng.targetSize = initialSize
	ng.maxSize = initialSize + 1

	t.Run("success", func(t *testing.T) {
		var wg sync.WaitGroup
		runThread := func(ng *magnumNodeGroup) {
			defer wg.Done()
			for i := 0; i < deletesPerThread; i++ {
				err := ng.DeleteNodes(nodesToDelete[:1])
				assert.NoError(t, err)
			}
		}
		for i := 0; i < threads; i++ {
			wg.Add(1)
			go runThread(ng)
		}
		wg.Wait()
		assert.Equal(t, *ng.targetSize, expectedFinalSize)
	})

}
