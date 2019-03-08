package btree

/*
 * B+树接口
 */
type Btree interface {
	AddBTree(treeName string)
	Set(treeName, key string, value uint64) error
	MutiSet(treeName string, kv map[string]string) error
	GetInt(treeName, key string) (int64, bool)
	Inc(treeName, key string)
	GetFristKV(treeName string) (string, uint32, uint32, int, bool)
	GetNextKV(treeName, key string) (string, uint32, uint32, int, bool)
	Close()
}
