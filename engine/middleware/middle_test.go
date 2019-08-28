package middleware

import (
	"testing"
	"sync"
	"reflect"
	"github.com/hq-cml/spider-engine/basic"
	"fmt"
)

func TestReqcache(t *testing.T) {
	rc := NewRequestCache()
	var wg sync.WaitGroup
	wg.Add(10)
	for i:=0; i<10; i++ {
		go func(i int, wg *sync.WaitGroup) int{
			rc.Put(basic.NewRequest(10, i))
			wg.Done()
			return 1
		}(i, &wg)
	}

	wg.Wait()
	i:=0
	var tmp *basic.SpiderRequest
	for {
		r := rc.Get()
		if reflect.TypeOf(tmp) != reflect.TypeOf(r) {
			panic("Type wrong")
		}
		t.Log("Got req", i, )
		i ++
		if rc.Length() == 0 {
			break;
		}
	}

	t.Log("End")
}

type Entity struct{
	Id int
}

func TestChannel(t *testing.T) {
	c := NewCommonChannel(5, "Test")
	var wg sync.WaitGroup

	for i:=0; i<10; i++ {
		go func(i int) {
			fmt.Println("Put ", i)
			_ = c.Put(Entity{i})
		}(i,)
	}

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		i := 0
		for {
			r, flag := c.Get()
			if !flag {
				fmt.Println("Closed!")
				break;
			}
			e, ok := r.(Entity)
			if !ok {
				break;
			}
			i ++
			if i == 10 {
				c.Close()
			}
			fmt.Println("Got entity", e.Id)
		}
		wg.Done()
	}(&wg)

	wg.Wait()

	t.Log("End")
}