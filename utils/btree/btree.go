package btree

import "github.com/hq-cml/spider-engine/utils/btree/boltbtree"

/*
 * B+树接口
 */
type Btree interface {
	AddTree(treeName string) error
	Set(treeName, key, value string) error
	MutiSet(treeName string, kv map[string]string) error
	GetInt(treeName, key string) (int64, bool)
	GetStr(treeName, key string) (string, bool)
	Inc(treeName, key string) error
	GetFristKV(treeName string) (string, uint32, bool)
	GetNextKV(treeName, key string) (string, uint32, bool)
	HasTree(treeName string) bool
	Close() error
	Display(treeName string) error
}

//工厂
//treeName用于工厂选择, 暂时没用
func NewBtree(treeClass, path string) Btree {
	return boltbtree.NewBoltBTree(path)
}
