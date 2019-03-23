package boltbtree

import (
	"testing"
)


func TestNewBoltTree(t *testing.T) {
	tree := GetBoltWrapperInstance()
	t.Log(tree.Tables)
	t.Log("\n\n")
}

func TestCreateTable(t *testing.T) {
	tree := GetBoltWrapperInstance()
	tree.CreateTable("second")
	t.Log("\n\n")
}

func TestDeleteTable(t *testing.T) {
	tree := GetBoltWrapperInstance()
	tree.DeleteTable("second")
	t.Log("\n\n")
}

func TestSetGet(t *testing.T) {
	tree := GetBoltWrapperInstance()
	if _, exist := tree.Tables["first"]; !exist {
		err := tree.CreateTable("first")
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tree.Set("first", "aa", "hello"); err != nil {
		t.Fatal(err)
	}
	v, ok := tree.Get("first", "aa")
	t.Log("Get: ",v, ok)
	t.Log("\n\n")
}

func TestSetNoExist(t *testing.T) {
	tree := GetBoltWrapperInstance()
	t.Log(tree.Set("not exist", "aa", "hello"))
	t.Log("\n\n")
}

func TestMultiSet(t *testing.T) {
	tree := GetBoltWrapperInstance()
	err := tree.MutiSet("first", map[string]string {
		"aa": "hello",
		"xx": "2",
		"ee": "3",
		"bb": "4",
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Log("\n\n")
}

func TestDisplayTable(t *testing.T) {
	tree := GetBoltWrapperInstance()
	tree.DisplayTable("first")

	t.Log("\n\n")
}

func TestGetFirst(t *testing.T) {
	tree := GetBoltWrapperInstance()
	k, v, e := tree.GetFristKV("first")
	if e != nil {
		t.Fatal(e)
	}
	t.Log(k, v, e)
	t.Log("\n\n")
}

func TestNextKV(t *testing.T) {
	tree := GetBoltWrapperInstance()
	k, v, e := tree.GetNextKV("first", "ee")
	if e != nil {
		t.Fatal(e)
	}
	t.Log(k, v, e )
	t.Log("\n\n")
}

/*

func TestNewBoltTree(t *testing.T) {
	InitBoltWrapper("/tmp/spider/spider.db", 0666, 3 * time.Second)
	t.Log(gBoltWrapper.Tables)
	t.Log("ok")
}

*/


