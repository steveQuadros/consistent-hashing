package main

import (
	"crypto/md5"
	"fmt"
	"math/big"
	"strconv"
)

type Node struct {
	id   int
	data []string
}

// https://docs.openstack.org/swift/latest/ring_background.html

const DataCount = 10_000_000

func main() {
	//RunBasic()
	OddEvenAddServer()
}

func RunBasic() {
	nodeCount := 100
	dataIDCount := DataCount

	nodes := make([]Node, nodeCount)

	var max int
	for i := 0; i < dataIDCount; i++ {
		hash := GetNodeID(strconv.Itoa(i))
		nodeID := *big.NewInt(0).Mod(hash, big.NewInt(int64(nodeCount)))
		nodes[nodeID.Uint64()].data = append(nodes[nodeID.Uint64()].data, strconv.Itoa(i))
		if len(nodes[nodeID.Uint64()].data) > max {
			max = len(nodes[nodeID.Uint64()].data)
		}
	}

	desiredCount := dataIDCount / nodeCount
	fmt.Printf("%d: desired IDs per node\n", desiredCount)
	fmt.Printf(
		"%d: max ids on any given node, %0.2f over\n",
		max,
		100.00*float64(max-int(desiredCount))/float64(desiredCount),
	)
}

// Demonstrates how many IDs will have to be moved if we add a single server
// It's around 99 percent of IDs for adding 1% new servers!
func OddEvenAddServer() {
	nodeCount := 100
	newNodeCount := 101
	dataIDCount := DataCount

	var movedIDs int
	for i := 0; i < dataIDCount; i++ {
		hash := GetNodeID(strconv.Itoa(i))
		nodeID := *big.NewInt(0).Mod(hash, big.NewInt(int64(nodeCount)))

		hash = GetNodeID(strconv.Itoa(i))
		newNodeID := *big.NewInt(0).Mod(hash, big.NewInt(int64(newNodeCount)))
		if nodeID.Cmp(&newNodeID) != 0 {
			movedIDs++
		}
	}
	percentMoved := float64(movedIDs) / float64(dataIDCount) * 100.00
	fmt.Printf("%d ids moved %0.2f percent", movedIDs, percentMoved)
}

// If instead we use hash ranges per node, we can drop the number of IDs moved when we add a new server
//
func HashRangesToLimitIDMoves() {
	nodeCount := 100
	newNodeCount := 101
	dataIDCount := DataCount
	var nodeRangeStarts []uint64
	for i := 0; i < nodeCount; i++ {
		idRange := dataIDCount / nodeCount * i
		nodeRangeStarts = append(nodeRangeStarts, uint64(idRange))
	}

	var newNodeRangeStarts []uint64
	for i := 0; i < newNodeCount; i++ {
		idRange := dataIDCount / nodeCount * i
		newNodeRangeStarts = append(newNodeRangeStarts, uint64(idRange))
	}

	bigDataIDCount := big.NewInt(int64(dataIDCount))
	var movedIDs int
	for i := 0; i < dataIDCount; i++ {
		hash := GetNodeID(strconv.Itoa(i))
		nodeID := BisectLeft(nodeRangeStarts, big.NewInt(0).Mod(hash, bigDataIDCount))

		hash = GetNodeID(strconv.Itoa(i))
		newNodeID := BisectLeft(nodeRangeStarts, big.NewInt(0).Mod(hash, bigDataIDCount))
		if nodeID.Cmp(&newNodeID) != 0 {
			movedIDs++
		}
	}
	percentMoved := float64(movedIDs) / float64(dataIDCount) * 100.00
	fmt.Printf("%d ids moved %0.2f percent", movedIDs, percentMoved)
}

// GetNodeID takes in a string, md5s it, and returns the bigint value of it
// see here for more information:
// https://stackoverflow.com/questions/28128285/best-way-to-convert-an-md5-to-decimal-in-golang
func GetNodeID(s string) *big.Int {
	h := md5.New()
	h.Write([]byte(s))
	hash := big.NewInt(0)
	hash.SetBytes(h.Sum(nil))
	return hash.Int64()
}

// @TODO
// - return Int64 / Uint64 val from GetNodeID
// - write Bisect Left method
// remove uses of BigInt and reaplce with Uint64 to we can use normal methods instead of bigint ones ( could be interesting to write a benchmark in the future)
