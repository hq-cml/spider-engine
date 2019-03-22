package btree

import "github.com/hq-cml/spider-engine/utils/btree/boltbtree"

/*
 * B+树接口
 */
type Btree interface {
	AddBTree(treeName string) error
	Set(treeName, key string, value uint64) error
	MutiSet(treeName string, kv map[string]string) error
	GetInt(treeName, key string) (int64, bool)
	Inc(treeName, key string) error
	GetFristKV(treeName string) (string, uint32, uint32, int, bool)
	GetNextKV(treeName, key string) (string, uint32, uint32, int, bool)
	Close() error
}

//工厂
//treeName用于工厂选择, 暂时没用
func NewBtree(treeName, path string) Btree {
	return boltbtree.NewBoltBTree(path)
}
