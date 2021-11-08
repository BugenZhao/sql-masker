package dict

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/zeebo/blake3"
)


func NewDictionary(context string, prefix string) *Dictionary {
	return &Dictionary{
		hasher: blake3.NewDeriveKey(context),
		prefix: prefix,
		dict:   make(map[string]uint32),
		values: make(map[uint32]bool),
	}
}

type Dictionary struct {
	hasher *blake3.Hasher
	prefix string

	dict   map[string]uint32
	values map[uint32]bool
}

func (d *Dictionary) get(key string) string {
	if value, ok := d.dict[key]; ok {
		return fmt.Sprintf("%s%s", d.prefix, strconv.FormatUint(uint64(value), 36))
	}
	return ""
}

func (d *Dictionary) Map(key string) string {
	value := d.get(key)
	if value != "" {
		return value
	}

	d.hasher.Reset()
	_, _ = d.hasher.Write([]byte(key))
	sum := make([]byte, 4)
	_, err := d.hasher.Digest().Read(sum)
	if err != nil {
		panic(err)
	}
	u := binary.LittleEndian.Uint32(sum)

	for {
		if d.values[u] {
			u += 1
			continue
		}
		d.values[u] = true
		d.dict[key] = u
		return d.get(key)
	}
}
