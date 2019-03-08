package idgen

import (
	"math"
	"sync"
)

//Id生成器
type IdGenerator struct {
	sn    uint64
	ended bool //是否已经达到最大的值
	mutex sync.Mutex
}

//惯例New
func NewIdGenerator() *IdGenerator {
	return &IdGenerator{}
}

//*IdGenerator实现IdGenerator接口
func (gen *IdGenerator) GetId() uint64 {
	gen.mutex.Lock()
	defer gen.mutex.Unlock()

	if gen.ended {
		gen.sn = 0
		gen.ended = false
		return gen.sn
	}

	id := gen.sn
	if id < math.MaxUint64 {
		gen.sn++
	} else {
		gen.ended = true
	}

	return id
}
