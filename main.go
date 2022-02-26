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
	//HashRangesToLimitIDMoves()
	VirtualNodes()
}

//func RunBasic() {
//	nodeCount := uint64(100)
//	dataIDCount := DataCount
//
//	nodes := make([]Node, nodeCount)
//
//	var max int
//	for i := 0; i < dataIDCount; i++ {
//		hash := GetNodeID(strconv.Itoa(i))
//		nodeID := hash % nodeCount
//		nodes[nodeID].data = append(nodes[nodeID].data, strconv.Itoa(i))
//		if len(nodes[nodeID].data) > max {
//			max = len(nodes[nodeID].data)
//		}
//	}
//
//	desiredCount := uint64(dataIDCount) / nodeCount
//	fmt.Printf("%d: desired IDs per node\n", desiredCount)
//	fmt.Printf(
//		"%d: max ids on any given node, %0.2f over\n",
//		max,
//		100.00*float64(max-int(desiredCount))/float64(desiredCount),
//	)
//}

// Demonstrates how many IDs will have to be moved if we add a single server
// It's around 99 percent of IDs for adding 1% new servers!
//func OddEvenAddServer() {
//	var nodeCount uint64 = 100
//	var newNodeCount uint64 = 101
//	dataIDCount := DataCount
//
//	var movedIDs int
//	for i := 0; i < dataIDCount; i++ {
//		hash := GetNodeID(strconv.Itoa(i))
//		nodeID := hash % nodeCount
//
//		hash = GetNodeID(strconv.Itoa(i))
//		newNodeID := hash % newNodeCount
//		if nodeID != newNodeID {
//			movedIDs++
//		}
//	}
//	percentMoved := float64(movedIDs) / float64(dataIDCount) * 100.00
//	fmt.Printf("%d ids moved %0.2f percent", movedIDs, percentMoved)
//}

// If instead we use hash ranges per node, we can drop the number of IDs moved when we add a new server
//
func HashRangesToLimitIDMoves() {
	var nodeCount uint64 = 100
	var newNodeCount uint64 = 101
	var dataIDCount uint64 = DataCount

	var nodeRangeStarts []uint64
	var i uint64
	for i = 0; i < nodeCount; i++ {
		idRange := dataIDCount / nodeCount * i
		nodeRangeStarts = append(nodeRangeStarts, idRange)
	}

	var newNodeRangeStarts []uint64
	for i = 0; i < newNodeCount; i++ {
		idRange := dataIDCount / newNodeCount * i
		newNodeRangeStarts = append(newNodeRangeStarts, idRange)
	}

	var movedIDs int
	bigDataIDCount := big.NewInt(int64(dataIDCount))
	mod := big.NewInt(0)

	for i = 0; i < dataIDCount; i++ {
		hash := GetNodeID(strconv.Itoa(int(i)))
		mod = mod.Mod(hash, bigDataIDCount)

		nodeIdx := BisectLeft(nodeRangeStarts, mod.Uint64())
		nodeID := nodeIdx % nodeCount

		newNodeIdx := BisectLeft(newNodeRangeStarts, mod.Uint64())
		newNodeID := newNodeIdx % newNodeCount
		if nodeID != newNodeID {
			movedIDs++
		}
	}

	percentMoved := float64(movedIDs) / float64(dataIDCount) * 100.00
	fmt.Printf("%d ids moved %0.2f percent", movedIDs, percentMoved)
}

func VirtualNodes() {
	nodeCount := 100
	vnodeCount := 1000
	dataIDCount := DataCount

	var vnodeRangeStarts []uint64
	var vnodeToNode []int
	for i := 0; i < vnodeCount; i++ {
		vnodeRangeStarts = append(vnodeRangeStarts, uint64(dataIDCount/vnodeCount*i))
		vnodeToNode = append(vnodeToNode, i%nodeCount)
	}

	newVnodeToNode := make([]int, len(vnodeToNode))
	copy(newVnodeToNode, vnodeToNode)
	newNodeID := nodeCount
	newNodeCount := nodeCount + 1
	vnodesToReassign := vnodeCount / newNodeCount

	for vnodesToReassign > 0 {
		for nodeToTakeFrom := 0; nodeToTakeFrom < nodeCount; nodeToTakeFrom++ {
			for vnodeID, nodeID := range newVnodeToNode {
				if nodeID == nodeToTakeFrom {
					newVnodeToNode[vnodeID] = newNodeID
					vnodesToReassign--
					break
				}
			}
			if vnodesToReassign <= 0 {
				break
			}
		}
	}

	var movedIDs int
	bigDataIDCount := big.NewInt(int64(dataIDCount))
	mod := big.NewInt(0)
	for dataID := 0; dataID < dataIDCount; dataID++ {
		hash := GetNodeID(strconv.Itoa(dataID))
		mod = mod.Mod(hash, bigDataIDCount)

		vnodeID := BisectLeft(vnodeRangeStarts, mod.Uint64()) % uint64(vnodeCount)
		nodeID := vnodeToNode[vnodeID]
		newNodeID = newVnodeToNode[vnodeID]
		if nodeID != newNodeID {
			movedIDs++
		}
	}
	percentMoved := float64(movedIDs) / float64(dataIDCount) * 100.00
	fmt.Printf("%d ids moved %0.2f percent", movedIDs, percentMoved)
}

// GetNodeID takes in a string, md5s it, and returns the bigint value of it
// see here for more information:
// https://stackoverflow.com/questions/28128285/best-way-to-convert-an-md5-to-decimal-in-golang
// Note we MUST return a bigint, as the value written by the md5.Sum() is much larger than uint64
func GetNodeID(s string) *big.Int {
	h := md5.New()
	h.Write([]byte(s))
	hash := big.NewInt(0)
	hash.SetBytes(h.Sum(nil))
	return hash
}

// in this case returns the index of the node onto which the data (target) should live
func BisectLeft(arr []uint64, target uint64) uint64 {
	left, right := 0, len(arr)-1

	for left <= right {
		mid := left + (right-left)/2
		if arr[mid] < target {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return uint64(left)
}
