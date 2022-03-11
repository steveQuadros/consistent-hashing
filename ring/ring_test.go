package ring

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math"
	"strconv"
	"testing"
	"time"
)

func TestRing(t *testing.T) {
	PartitionPower := 16
	Replicas := 3
	NodeCount := 256
	ZoneCount := 16
	ring := New(NodeCount, ZoneCount, PartitionPower, Replicas)
	testZones(t, ring)
	testNodePartitions(t, ring, NodeCount, PartitionPower)
	testGetNodes(t, ring, Replicas)
}

func testZones(t *testing.T, ring Ring) {
	// there should be equal representation amongst the nodes since everything is power of 2
	nodeCountPerZone := ring.NodeCount() / ring.ZoneCount()
	zones := map[int]int{}
	for _, n := range ring.Nodes() {
		zones[n.zone]++
	}
	for _, c := range zones {
		require.Equal(t, nodeCountPerZone, c)
	}
}

func testNodePartitions(t *testing.T, ring Ring, nodeCount, partitionPower int) {
	// each node should have the same number of virtual nodes
	partitionsPerNode := pow2(partitionPower) / nodeCount
	nodePartitionCount := map[int]int{}
	for _, nodeID := range ring.Partitions() {
		nodePartitionCount[nodeID]++
	}

	for id, c := range nodePartitionCount {
		require.Equal(t, partitionsPerNode, c, fmt.Sprintf("node: %d incorrect node count", id))
	}
}

func testGetNodes(t *testing.T, ring Ring, replicas int) {
	// get nodes should return REPLICA number of different nodes to write to
	nodes := ring.GetNodes(0)
	require.Equal(t, replicas, len(nodes))

	// ids need to be unique
	nodeIDs := map[int]struct{}{}
	for _, n := range nodes {
		nodeIDs[n.id] = struct{}{}
	}

	require.Equal(t, replicas, len(nodeIDs))
}

func BenchmarkRing(b *testing.B) {
	start := time.Now()

	PartitionPower := 16
	Replicas := 3
	NodeCount := 256
	ZoneCount := 16
	ring := New(NodeCount, ZoneCount, PartitionPower, Replicas)

	dataIDCount := 10000000
	nodeCounts := map[int]int{}
	zoneCounts := map[int]int{}

	for i := 0; i < dataIDCount; i++ {
		nodes := ring.GetNodes(i)
		for j := range nodes {
			nodeCounts[nodes[j].id]++
			zoneCounts[nodes[j].zone]++
		}
	}

	b.Log(fmt.Sprintf("%0.1fs to test ring", time.Since(start).Seconds()))

	desiredCount := float64(dataIDCount) / float64(NodeCount) * float64(ring.replicas)
	b.Log(fmt.Sprintf("%d: desired data ids per node", int(desiredCount)))

	var maxCount int
	for i := 0; i < len(nodeCounts); i++ {
		if nodeCounts[i] > maxCount {
			maxCount = nodeCounts[i]
		}
	}

	over := 100.0 * (float64(maxCount-int(desiredCount)) / desiredCount)
	b.Log(fmt.Sprintf("%d: most data ids on one node, %.02f%% over", maxCount, over))

	minCount := math.MaxInt64
	for i := 0; i < len(nodeCounts); i++ {
		if nodeCounts[i] < minCount {
			minCount = nodeCounts[i]
		}
	}
	under := 100.0 * (desiredCount - float64(minCount)) / float64(desiredCount)
	b.Log(fmt.Sprintf("%d: least data ids on one node, %.02f%% under", minCount, under))

	zonesInNodes := make(map[int]struct{})
	for _, n := range ring.nodes {
		zonesInNodes[n.zone] = struct{}{}
	}

	desiredZones := float64(dataIDCount) / float64(ZoneCount*ring.replicas)
	b.Log(fmt.Sprintf("%d: desired data ids per zone", int(desiredZones)))
	for _, c := range zoneCounts {
		if c > maxCount {
			maxCount = c
		}
	}
	over = 100.0 * ((float64(maxCount) - desiredZones) / desiredCount)
	b.Log(fmt.Sprintf("%d: most data ids in one zone, %.02f%% over", maxCount, over))

	minCount = math.MaxInt32
	for _, c := range zoneCounts {
		if c < minCount {
			minCount = c
		}
	}

	under = 100.0 * ((desiredZones - float64(minCount)) / desiredCount)
	b.Log(fmt.Sprintf("%d: least data ids in one zone, %.02f%% under", minCount, under))
}

// TestGetID verifies behavior is in line with python implementation
// ex:
// >>> unpack_from('>I', md5(str(2)).digest())
//     (3357438605,)
// >>> unpack_from('>I', md5(str(1)).digest())
//     (3301589560,)
// >>> unpack_from('>I', md5(str(999)).digest())
//     (3070657373,)
func TestGetID(t *testing.T) {
	tc := []struct {
		in  int
		out uint32
	}{
		{999, 3070657373},
		{1, 3301589560},
		{2, 3357438605},
	}

	for _, tt := range tc {
		t.Run(fmt.Sprintf("%d -> %d", tt.in, tt.out), func(t *testing.T) {
			actual := GetNodeID(tt.in)
			require.Equal(t, tt.out, actual, fmt.Sprintf("%d -> %d", tt.out, actual))
		})
	}
}

func TestPow(t *testing.T) {
	tc := []struct {
		in, out int
	}{
		{2, 4},
		{3, 8},
		{4, 16},
		{32, 4294967296},
	}

	for _, tt := range tc {
		t.Run(strconv.Itoa(tt.in), func(t *testing.T) {
			require.Equal(t, tt.out, pow2(tt.in))
		})
	}
}

func BenchmarkPowShift(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pow2(i)
	}
}

func BenchmarkMathPow(b *testing.B) {
	var i float64
	for ; int(i) < b.N; i++ {
		math.Pow(2.0, i)
	}
}
