package ring

import (
	"errors"
	"fmt"
	fake "github.com/brianvoe/gofakeit"
	"math"
	"testing"
)

func TestNodes(t *testing.T) {
	partitionPower := 16
	replicas := 3
	nodeCount := 256
	zoneCount := 16
	nodes := InitNodes(nodeCount, zoneCount)
	ring := New(nodes, zoneCount, partitionPower, replicas)

	dataCount := 10000

	var creates, updates int
	for i := 0; i < dataCount; i++ {
		s := fake.Name()
		targets := ring.GetNodes(s)
		addr := fake.BeerAlcohol()
		for _, n := range targets {
			// @TODO look up redis handling of setting key cmds create vs update
			err := n.Set(s, addr)
			if errors.Is(err, KeyExistsError{}) {
				_ = n.Update(s, addr)
				updates++
			} else {
				creates++
			}
		}
	}

	t.Log(fmt.Sprintf("%d keys created, %d updated", creates, updates))
	perNode := (dataCount / nodeCount) * replicas
	t.Log(fmt.Sprintf("%d data expected per node", perNode))

	var maxDataOnNode int
	minDataOnNode := math.MaxInt64
	for i := 0; i < nodeCount; i++ {
		n := nodes[i]
		if len(n.Data) > maxDataOnNode {
			maxDataOnNode = len(n.Data)
		}
		if len(n.Data) < minDataOnNode {
			minDataOnNode = len(n.Data)
		}
	}

	t.Log(fmt.Sprintf(
		"Max on node: %d %.02f over; min on node %d, %.02f under",
		maxDataOnNode,
		float64(maxDataOnNode)/float64(perNode),
		minDataOnNode,
		float64(minDataOnNode)/float64(perNode),
	))
}
