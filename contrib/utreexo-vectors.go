// Run from an Utreexod checkout using github.com/utreexo/utreexo v0.18.0:
// go run /path/to/rpc-utreexo-bridge/contrib/utreexo-vectors.go
// Output is the independent reference fixture in tests/data/utreexo.json.
package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"

	"github.com/utreexo/utreexo"
	"github.com/utreexo/utreexod/wire"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func hexes(hashes []utreexo.Hash) []string {
	result := make([]string, len(hashes))
	for i, hash := range hashes {
		result[i] = hex.EncodeToString(hash[:])
	}
	return result
}

func main() {
	hashes := make([]utreexo.Hash, 11)
	for i := range hashes {
		leaf := wire.LeafData{Height: int32(i + 100), Amount: int64(i + 1000), IsCoinBase: i%2 == 0, PkScript: []byte{0x51, byte(i)}}
		leaf.OutPoint.Index = uint32(i * 3)
		for j := 0; j < 32; j++ {
			leaf.BlockHash[j] = byte(i + j)
			leaf.OutPoint.Hash[j] = byte(i + j + 7)
		}
		hashes[i] = leaf.LeafHash()
	}
	p := utreexo.NewAccumulator()
	type Step struct {
		Adds, Dels, Prove []int
		Leaves            uint64
		Roots, Proof      []string
		Targets           []uint64
		RootMessage       string
	}
	steps := []Step{
		{Adds: []int{0, 1, 2, 3, 4, 5, 6, 7}, Dels: []int{}, Prove: []int{1, 3, 7}},
		{Adds: []int{}, Dels: []int{0, 2}, Prove: []int{1, 3, 7}},
		{Adds: []int{8, 9, 10}, Dels: []int{4, 5}, Prove: []int{1, 7, 10}},
		{Adds: []int{}, Dels: []int{10}, Prove: []int{1, 7, 8}},
		{Adds: []int{}, Dels: []int{1, 3, 6, 7, 8, 9}, Prove: []int{}},
	}
	for i := range steps {
		step := &steps[i]
		adds := make([]utreexo.Leaf, len(step.Adds))
		for j, index := range step.Adds {
			adds[j] = utreexo.Leaf{Hash: hashes[index], Remember: true}
		}
		dels := make([]utreexo.Hash, len(step.Dels))
		for j, index := range step.Dels {
			dels[j] = hashes[index]
		}
		proof, err := p.Prove(dels)
		check(err)
		check(p.Modify(adds, dels, proof))
		prove := make([]utreexo.Hash, len(step.Prove))
		for j, index := range step.Prove {
			prove[j] = hashes[index]
		}
		proof, err = p.Prove(prove)
		check(err)
		step.Leaves, step.Roots, step.Proof, step.Targets = p.GetNumLeaves(), hexes(p.GetRoots()), hexes(proof.Proof), proof.Targets
		message := wire.MsgUtreexoRoot{NumLeaves: step.Leaves, Roots: p.GetRoots()}
		var buffer bytes.Buffer
		check(message.BtcEncode(&buffer, 70016, wire.LatestEncoding))
		step.RootMessage = hex.EncodeToString(buffer.Bytes())
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	check(encoder.Encode(map[string]interface{}{"leaf_hashes": hexes(hashes), "steps": steps}))
}
