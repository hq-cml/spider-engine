package boltbtree

import (
	"testing"
)


func TestNewBoltTree(t *testing.T) {
	tree := GetBoltWrapperInstance()
	t.Log(tree.Buckets)
	t.Log("\n\n")
}

func TestCreateBucket(t *testing.T) {
	tree := GetBoltWrapperInstance()
	tree.CreateBucket("second")
	t.Log("\n\n")
}

func TestDeleteBucket(t *testing.T) {
	tree := GetBoltWrapperInstance()
	tree.DeleteBucket("second")
	t.Log("\n\n")
}

func TestSetGet(t *testing.T) {
	tree := GetBoltWrapperInstance()
	if _, exist := tree.Buckets["first"]; !exist {
		err := tree.CreateBucket("first")
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

func TestDisplayBucket(t *testing.T) {
	tree := GetBoltWrapperInstance()
	tree.DisplayBucket("first")

	t.Log("\n\n")
}

func TestGetFirst(t *testing.T) {
	tree := GetBoltWrapperInstance()
	k, v, e := tree.GetFristKV("first")
	if e != nil {
		t.Fatal(e)
	}
	t.Log(k, v, e)

	//多次调用first，得到的值相同，不会帮你自动后移cursor ~
	k, v, e = tree.GetFristKV("first")
	if e != nil {
		t.Fatal(e)
	}
	t.Log(k, v, e)

	k, v, e = tree.GetFristKV("first")
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

	//测试不存在
	k, v, e = tree.GetNextKV("first", "haha")
	t.Log(k, v, e )

	//测试最后一个
	var ee error
	k, v, ee = tree.GetNextKV("first", "xx")
	t.Log(k, v, ee )
	v, ok := tree.Get("first", "xx")
	t.Log("Get: ",v, ok)


	t.Log("\n\n")
}

/*

func TestNewBoltTree(t *testing.T) {
	InitBoltWrapper("/tmp/spider/spider.btdb", 0666, 3 * time.Second)
	t.Log(gBoltWrapper.Buckets)
	t.Log("ok")
}

*/


