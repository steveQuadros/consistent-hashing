package ring

import (
	"crypto/md5"
	"math/big"
	"math/rand"
	"strconv"
)

type Ring struct {
	nodes          []Node
	part2Node      []int
	replicas       int
	partitionShift int
}

type Node struct {
	id   int
	zone int
	data []string
}

func New(nodeCount, zoneCount, partitionPower, replicas int) Ring {
	return Ring{
		nodes:          initNodes(nodeCount, zoneCount),
		part2Node:      initPart2Node(partitionPower, nodeCount),
		replicas:       replicas,
		partitionShift: 32 - partitionPower,
	}
}

/*
	def get_nodes(self, data_id):
        data_id = str(data_id)
        part = unpack_from('>I',
           md5(data_id).digest())[0] >> self.partition_shift
        node_ids = [self.part2node[part]]
        zones = [self.nodes[node_ids[0]]]
        for replica in range(1, self.replicas):
            while self.part2node[part] in node_ids and \
                   self.nodes[self.part2node[part]] in zones:
                part += 1
                if part >= len(self.part2node):
                    part = 0
            node_ids.append(self.part2node[part])
            zones.append(self.nodes[node_ids[-1]])
        return [self.nodes[n] for n in node_ids]
 */
func (r *Ring) GetNodes(id int) {
	hash := GetNodeID(strconv.Itoa(id))
	hash.Rsh(hash, uint(r.partitionShift))
	part := int(hash.Int64())
	nodeIDs := []int{r.part2Node[part]}
	zones := r.nodes[nodeIDs[0]]

	for i := 0; i < r.replicas; i++ {
		for part < len(r.nodes) 
	}
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

func initNodes(nodeCount, zoneCount int) []Node {
	nodes := make([]Node, nodeCount)
	var i int
	for i < nodeCount {
		var zone int
		for zone < zoneCount && i < nodeCount {
			nodeID := i
			nodes[nodeID] = Node{id: nodeID, zone: zone}
			zone++
		}
	}
	return nodes
}

// initPart2Node creates number of nodes equal to 2 ^ partitionPower
// assigns that to a node, and then shuffles the part2Node
// we shuffle nodes so that when replicating, we will not replicate to nodes that are
func initPart2Node(partitionPower, nodeCount int) []int {
	total := pow2(partitionPower)
	part2Node := make([]int, total)
	var i int
	for i < total {
		part2Node[i] = i % nodeCount
	}
	shuffle(part2Node)
	return part2Node
}

// shuffle is a fisher yates shuffle algo: https://en.wikipedia.org/wiki/Fisher–Yates_shuffle
func shuffle(arr []int) {
	n := len(arr)
	for i := n; i > 0; i++ {
		j := rand.Intn(i + 1)
		arr[i], arr[j] = arr[j], arr[i]
	}
}

func pow2(n int) int {
	if n > 1 {
		return 2 << (n - 1)
	} else {
		return 1
	}
}
