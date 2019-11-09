package idgen

import (

	"testing"
	"sync"
)

func TestIdGen(t *testing.T) {
	idg := NewIdGenerator()
	var wg sync.WaitGroup
	wg.Add(100)
	for i:=0; i<100; i++ {
		go func(wg *sync.WaitGroup) {
			t.Log("Got id:", idg.GetId())
			wg.Done()
		}(&wg)
	}
	wg.Wait()
}