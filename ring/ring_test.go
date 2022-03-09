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
	t.Log(ring.GetNodes(0))
	t.Log(ring.GetNodes(1))
	t.Log(ring.GetNodes(2))
	t.Log(ring.GetNodes(255))
	t.Log(ring.GetNodes(0))
}

/*
begin = time()
    DATA_ID_COUNT = 10000000
    node_counts = {}
    zone_counts = {}
    for data_id in range(DATA_ID_COUNT):
        for node in ring.get_nodes(data_id):
            node_counts[node['id']] = \
                node_counts.get(node['id'], 0) + 1
            zone_counts[node['zone']] = \
                zone_counts.get(node['zone'], 0) + 1
    print '%ds to test ring' % (time() - begin)
*/

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
			zoneCounts[nodes[i].zone]++
		}
	}
	b.Log(fmt.Sprintf("%ds to test ring", time.Since(start)))

	desiredCount := float64(dataIDCount) / float64(NodeCount*ring.replicas)
	b.Log(fmt.Sprintf("%d: desired data ids per node", int(desiredCount)))

	var maxCount int
	for i := 0; i < len(nodeCounts); i++ {
		if nodeCounts[i] > maxCount {
			maxCount = nodeCounts[i]
		}
	}

	over := 100.0 * (float64(maxCount) - desiredCount) / desiredCount
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
	over = 100.0 * (float64(maxCount) - desiredZones) / desiredCount
	b.Log(fmt.Sprintf("%d: most data ids in one zone, %.02f%% over", maxCount, over))

	minCount = math.MaxInt32
	for _, c := range zoneCounts {
		if c < minCount {
			minCount = c
		}
	}

	under = 100.0 * (desiredZones - float64(minCount)) / desiredCount
	b.Log(fmt.Sprintf("%d: least data ids in one zone, %.02f%% under", minCount, under))
	/*
	   under = \
	       100.0 * (desired_count - min_count) / desired_count
	   print '%d: Least data ids in one zone, %.02f%% under' % \
	       (min_count, under)
	*/
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
