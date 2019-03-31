package boltbtree
/*
 * 基于bolt封装的B+树的实现
 * 这里的一个tree，就对应底层bolt的一个bucket！！
 */
import (
	"fmt"
	"strconv"
	"time"
	"github.com/hq-cml/spider-engine/utils/log"
)

type BoltBTree struct {
	filename string
	wrapper  *BoltWrapper
}

func NewBoltBTree(filename string) *BoltBTree {
	bt := &BoltBTree {
		filename: filename,
	}
	var err error
	//log.Info("Begin to Open: filename")
	bt.wrapper, err = NewBoltWrapper(filename, 0666, 5 * time.Second)
	if err != nil {
		log.Fatal("NewBoltBTree Error:", err)
	}
	//log.Info("End Open: filename")

	return bt

}

//增加一棵树, 底层对应 => 一个wrapper.Table => 一个bolt.bucket
func (bt *BoltBTree) AddTree(treeName string) error {
	return bt.wrapper.CreateBucket(treeName)
}

//Set
func (bt *BoltBTree) Set(treeName, key string, value uint64) error {
	return bt.wrapper.Set(treeName, key, fmt.Sprintf("%v", value))
}

//Multi Set
func (bt *BoltBTree) MutiSet(treeName string, kv map[string]string) error {
	return bt.wrapper.MutiSet(treeName, kv)
}

//get int
func (bt *BoltBTree) GetInt(treeName, key string) (int64, bool) {
	vstr, ok := bt.wrapper.Get(treeName, key)
	if !ok {
		return 0, false
	}

	i, e := strconv.ParseInt(vstr, 10, 64)
	if e != nil {
		return 0, false
	}
	return i, true
}

//inc
func (bt *BoltBTree) Inc(treeName, key string) error {
	v, ok := bt.GetInt(treeName, key)
	if !ok {
		v = 1
	} else {
		v++
	}

	return bt.wrapper.Set(treeName, key, fmt.Sprintf("%v", v))
}

//TODO 重构, 返回值太多
func (bt *BoltBTree) GetFristKV(treeName string) (string, uint32, uint32, int, bool) {
	key, vstr, err := bt.wrapper.GetFristKV(treeName)
	if err != nil {
		return "", 0, 0, 0, false
	}
	//db.logger.Info("Search btname : %v  key : %v value str : %v ",btname,key,vstr)
	u, e := strconv.ParseUint(vstr, 10, 64)
	if e != nil {
		return "", 0, 0, 0, false
	}
	//db.logger.Info("Search btname : %v  key : %v value  : %v ",btname,key,u)
	return key, uint32(u), 0, 0, true
}

func (db *BoltBTree) GetNextKV(treeName, key string) (string, uint32, uint32, int, bool) {

	vkey, vstr, err := db.wrapper.GetNextKV(treeName, key)
	if err != nil {
		return "", 0, 0, 0, false
	}

	u, e := strconv.ParseUint(vstr, 10, 64)
	if e != nil {
		return "", 0, 0, 0, false
	}
	return vkey, uint32(u), 0, 0, true

}

func (db *BoltBTree) HasTree(treeName string) bool {
	return db.wrapper.HasBucket(treeName)
}

//close
func (db *BoltBTree) Close() error {
	return db.wrapper.CloseDB()
}