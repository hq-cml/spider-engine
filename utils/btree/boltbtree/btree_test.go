package boltbtree

import (
	"testing"
	"time"
)


func TestNewBoltTree(t *testing.T) {
	InitBoltWrapper("/tmp/spider.db", 0666, 3 * time.Second)
	t.Log(gBoltWrapper.Tables)
	t.Log("ok")
}

func TestCreateTable(t *testing.T) {
	InitBoltWrapper("/tmp/spider.db", 0666, 3 * time.Second)
	gBoltWrapper.CreateTable("second")
	t.Log("ok")
}

func TestDeleteTable(t *testing.T) {
	InitBoltWrapper("/tmp/spider.db", 0666, 3 * time.Second)
	gBoltWrapper.DeleteTable("second")
	t.Log("ok")
}

func TestSetGet(t *testing.T) {
	InitBoltWrapper("/tmp/spider.db", 0666, 3 * time.Second)
	gBoltWrapper.Set("first", "aa", "hello")
	v, ok := gBoltWrapper.Get("first", "aa")
	t.Log("Get: ",v, ok)
	t.Log("ok")
}

func TestSetNoExist(t *testing.T) {
	InitBoltWrapper("/tmp/spider.db", 0666, 3 * time.Second)
	t.Log(gBoltWrapper.Set("not exist", "aa", "hello"))
}

func TestMultiSet(t *testing.T) {
	InitBoltWrapper("/tmp/spider.db", 0666, 3 * time.Second)
	gBoltWrapper.MutiSet("first", map[string]string {
		"aa": "hello",
		"xx": "2",
		"ee": "3",
		"bb": "4",
	})

	t.Log("ok")
}

func TestDisplayTable(t *testing.T) {
	InitBoltWrapper("/tmp/spider.db", 0666, 3 * time.Second)
	gBoltWrapper.DisplayTable("first")

	t.Log("ok")
}

func TestGetFirst(t *testing.T) {
	InitBoltWrapper("/tmp/spider.db", 0666, 3 * time.Second)
	k, v, e := gBoltWrapper.GetFristKV("first")
	t.Log(k, v, e )
}

func TestNextKV(t *testing.T) {
	InitBoltWrapper("/tmp/spider.db", 0666, 3 * time.Second)
	k, v, e := gBoltWrapper.GetNextKV("first", "ee")
	t.Log(k, v, e )
}

/*

func TestNewBoltTree(t *testing.T) {
	InitBoltWrapper("/tmp/spider/spider.db", 0666, 3 * time.Second)
	t.Log(gBoltWrapper.Tables)
	t.Log("ok")
}

*/


