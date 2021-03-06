package ring

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math/rand"
)

type Ring struct {
	nodes          []*Node
	part2Node      []int
	replicas       int
	partitionShift int
	zoneCount      int
}

type Node struct {
	ID   int
	Zone int
	Data map[string]string
}

type KeyExistsError struct {
	Key, CurrentVal string
}

func (e KeyExistsError) Error() string {
	return fmt.Sprintf("key %q already exists with value %q", e.Key, e.CurrentVal)
}

type NoSuchKeyError struct {
	Key string
}

func (e NoSuchKeyError) Error() string {
	return fmt.Sprintf("key %q does not exist", e.Key)
}

func (n *Node) Set(k, v string) error {
	if val, exists := n.Data[k]; exists {
		return KeyExistsError{Key: k, CurrentVal: val}
	}
	n.Data[k] = v
	return nil
}

func (n *Node) Update(k, v string) error {
	if _, exists := n.Data[k]; !exists {
		return NoSuchKeyError{Key: k}
	}
	n.Data[k] = v
	return nil
}

func New(nodes []*Node, zoneCount, partitionPower, replicas int) Ring {
	return Ring{
		nodes:          nodes,
		part2Node:      initPart2Node(partitionPower, len(nodes)),
		replicas:       replicas,
		partitionShift: 32 - partitionPower,
		zoneCount:      zoneCount,
	}
}

func (r *Ring) Nodes() []*Node {
	return r.nodes
}

func (r *Ring) Partitions() []int {
	return r.part2Node
}

func (r *Ring) NodeCount() int {
	return len(r.nodes)
}

func (r *Ring) ZoneCount() int {
	return r.zoneCount
}

func (r *Ring) PartitionCount() int {
	return len(r.part2Node)
}

func (r *Ring) GetNodes(id string) []*Node {
	hash := GetNodeID(id)
	hash = hash >> r.partitionShift
	part := int(hash)
	nodeIDs := []int{r.part2Node[part]}
	zones := []*Node{r.nodes[nodeIDs[0]]}

	// replicate to number of replica nodes
	for replica := 1; replica < r.replicas; replica++ {
		for contains(r.part2Node[part], nodeIDs) && containsNode(r.nodes[r.part2Node[part]], zones) {
			part++
			if part >= len(r.part2Node) {
				part = 0
			}
		}
		nodeIDs = append(nodeIDs, r.part2Node[part])
		zones = append(zones, r.nodes[nodeIDs[len(nodeIDs)-1]])
	}

	nodes := make([]*Node, len(nodeIDs))
	for i := range nodeIDs {
		nodes[i] = r.nodes[nodeIDs[i]]
	}
	return nodes
}

func contains(n int, arr []int) bool {
	for i := range arr {
		if n == arr[i] {
			return true
		}
	}
	return false
}

func containsNode(n *Node, nodes []*Node) bool {
	for i := range nodes {
		if nodes[i].ID == n.ID {
			return true
		}
	}
	return false
}

// GetNodeID takes in a string, md5s it, and returns a 32 bit value of it truncated
// see here for more information:
// https://stackoverflow.com/questions/28128285/best-way-to-convert-an-md5-to-decimal-in-golang
func GetNodeID(s string) uint32 {
	h := md5.New()
	h.Write([]byte(s))
	sum := h.Sum(nil)
	// we cut to 4 most significant bytes to return a 32 bit int ala what unpack_from(">I"...) does
	// see calcsize on unpack_from page https://docs.python.org/3/library/struct.html#struct.calcsize
	sum = sum[:4]
	return binary.BigEndian.Uint32(sum)
}

func InitNodes(nodeCount, zoneCount int) []*Node {
	nodes := make([]*Node, nodeCount)
	var i int
	for i < nodeCount {
		var zone int
		for zone < zoneCount && i < nodeCount {
			nodeID := i
			nodes[i] = &Node{ID: nodeID, Zone: zone, Data: make(map[string]string)}
			zone++
			i++
		}
	}
	return nodes
}

// initPart2Node creates number of nodes equal to 2 ^ partitionPower
// assigns that to a node, and then shuffles the part2Node
// we shuffle nodes so that when replicating, we will not replicate to nodes that are adjacent
func initPart2Node(partitionPower, nodeCount int) []int {
	total := pow2(partitionPower)
	part2Node := make([]int, total)
	var i int
	for i < total {
		part2Node[i] = i % nodeCount
		i++
	}
	shuffle(part2Node)
	return part2Node
}

// shuffle is a fisher yates shuffle algo: https://en.wikipedia.org/wiki/Fisher???Yates_shuffle
func shuffle(arr []int) {
	n := len(arr) - 1
	for i := n; i > 0; i-- {
		j := rand.Intn(i)
		arr[i], arr[j] = arr[j], arr[i]
	}
}

func pow2(n int) int {
	if n > 1 {
		return 1 << n
	} else {
		return 1
	}
}
