package index

import (
	"os"
	"os/exec"
	"testing"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/mmap"
	"github.com/hq-cml/spider-engine/utils/btree"
	"encoding/json"
)

const TEST_TREE = "user_name"

func init() {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
}

func TestGetDocNodeSize(t *testing.T) {
	t.Log("DocNode Size:", basic.DOC_NODE_SIZE)
	t.Log("\n\n")
}

func TestSplitWordsRune(t *testing.T) {
	ret := SplitRuneWords(0, "我爱北京天安门, Hello world!")
	r, _ := json.Marshal(ret)
	t.Log(string(r))
	t.Log("\n\n")
}

func TestSplitWords(t *testing.T) {
	ret := SplitTrueWords(0, "我爱北京天安门, Hello world!")
	r, _ := json.Marshal(ret)
	t.Log(string(r))
	t.Log("\n\n")
}

//********************* 倒排索引 *********************
func TestAddDoc(t *testing.T) {
	rIdx := NewEmptyInvertedIndex(IDX_TYPE_STR_SPLITER, 0, TEST_TREE)
	rIdx.AddDocument(0, "我爱北京天安门")
	rIdx.AddDocument(1, "天安门上太阳升")
	rIdx.AddDocument(2, "火红的太阳")
	r, _ := json.Marshal(rIdx.termMap)
	t.Log(string(r))
	t.Log("\n\n")
}

func TestQureyTermInMemAndPersist(t *testing.T) {
	rIdx := NewEmptyInvertedIndex(IDX_TYPE_STR_SPLITER, 0, TEST_TREE)
	rIdx.AddDocument(0, "我爱北京天安门")
	rIdx.AddDocument(1, "天安门上太阳升")
	rIdx.AddDocument(2, "火红的太阳")
	r, _ := json.Marshal(rIdx.termMap)
	t.Log(string(r))

	//从内存读取
	nodes, exist := rIdx.QueryTerm("天安门")
	if !exist {
		panic("Wrong exist")
	}
	n, _ := json.Marshal(nodes)
	t.Log("从内存访问: ", string(n))
	if len(nodes)!=2 {
		panic("Wrong somthing")
	}

	//测试落地
	tree := btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer tree.Close()
	if err := rIdx.Persist("/tmp/spider/Partition0", tree); err != nil {
		panic(err)
	}
	t.Log("\n\n")
}

func TestQureyTermInFile(t *testing.T) {
	//新建索引
	rIdx := NewEmptyInvertedIndex(IDX_TYPE_STR_SPLITER, 0, TEST_TREE)
	rIdx.inMemory = false //写死, 强制走磁盘

	//从磁盘加载btree
	//InitBoltWrapper("/tmp/spider/spider.db", 0666, 3 * time.Second)
	rIdx.btdb = btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer rIdx.btdb.Close()
	//从磁盘加载mmap
	var err error
	rIdx.ivtMmap, err = mmap.NewMmap("/tmp/spider/Partition0" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		panic(err)
	}
	nodes, exist := rIdx.QueryTerm("天安门")
	if !exist {
		panic("Wrong exist")
	}
	n, _ := json.Marshal(nodes)
	t.Log("从磁盘访问 天安门: ", string(n))

	nodes, exist = rIdx.QueryTerm("太阳")
	if !exist {
		panic("Wrong exist")
	}
	n, _ = json.Marshal(nodes)
	t.Log("从磁盘访问 太阳: ", string(n))

	t.Log("\n\n")
}

func TestMergeIndex(t *testing.T) {
	//清空目录
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}

	//建一颗B+树 => 建立索引1 => 落地索引1 => 再加载索引1
	tree1 := btree.NewBtree("xx", "/tmp/spider/spider_1" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer tree1.Close()
	rIdx1 := NewEmptyInvertedIndex(IDX_TYPE_STR_LIST, 0, TEST_TREE)
	rIdx1.AddDocument(0, "c;f")
	rIdx1.AddDocument(1, "a;c")
	rIdx1.AddDocument(2, "f;a")
	r, _ := json.Marshal(rIdx1.termMap)
	rIdx1.Persist("/tmp/spider/Partition_1", tree1) //落地
	t.Log(string(r))
	rIdx1.ivtMmap, err = mmap.NewMmap("/tmp/spider/Partition_1" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0) //加载
	if err != nil {
		panic(err)
	}

	//建一颗B+树 => 建立索引2 => 落地索引2 => 再加载索引2
	tree2 := btree.NewBtree("xx", "/tmp/spider/spider_2" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer tree2.Close()
	rIdx2 := NewEmptyInvertedIndex(IDX_TYPE_STR_LIST, 3, TEST_TREE)
	rIdx2.AddDocument(3, "b;d")
	rIdx2.AddDocument(4, "d;c")
	rIdx2.AddDocument(5, "b;c")
	r, _ = json.Marshal(rIdx2.termMap)
	rIdx2.Persist("/tmp/spider/Partition_2", tree2) //落地
	t.Log(string(r))
	rIdx2.ivtMmap, err = mmap.NewMmap("/tmp/spider/Partition_2" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0) //加载
	if err != nil {
		panic(err)
	}

	//建一颗B+树 => 建立索引3 => 落地索引3 => 再加载索引3
	tree3 := btree.NewBtree("xx", "/tmp/spider/spider_3" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer tree3.Close()
	rIdx3 := NewEmptyInvertedIndex(IDX_TYPE_STR_LIST, 6, TEST_TREE)
	rIdx3.AddDocument(6, "c;e")
	rIdx3.AddDocument(7, "a;e")
	rIdx3.AddDocument(8, "c;a")
	r, _ = json.Marshal(rIdx3.termMap)
	rIdx3.Persist("/tmp/spider/Partition_3", tree3) //落地
	t.Log(string(r))
	rIdx3.ivtMmap, err = mmap.NewMmap("/tmp/spider/Partition_3" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0) //加载
	if err != nil {
		panic(err)
	}

	//合并 => 读一下试一下
	tree := btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer tree.Close()
	rIdx0 := NewEmptyInvertedIndex(IDX_TYPE_STR_LIST, 0, TEST_TREE)
	err = rIdx0.MergePersistIvtIndex([]*InvertedIndex{rIdx1, rIdx2, rIdx3}, "/tmp/spider/Partition", tree)
	if err != nil {
		panic(err)
	}
	ivtMmap, err := mmap.NewMmap("/tmp/spider/Partition" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		panic(err)
	}
	rIdx0.SetIvtMmap(ivtMmap) //必须设置mmap
	term, _, ok := rIdx0.btdb.GetFristKV(TEST_TREE)
	for ok {
		nodes, exist := rIdx0.QueryTerm(term)
		if !exist {
			panic(term)
		}
		n, _ := json.Marshal(nodes)
		t.Log("从磁盘访问 ", term ,": ", string(n))

		term, _, ok = rIdx0.btdb.GetNextKV(TEST_TREE, term)
	}
	t.Log("NextId:", rIdx0.nextDocId)

	//加载回来 => 读一下试一下
	rIdx := LoadInvertedIndex(tree, IDX_TYPE_STR_LIST, TEST_TREE, ivtMmap, 9)
	term, _, ok = rIdx.btdb.GetFristKV(TEST_TREE)
	for ok {
		nodes, exist := rIdx.QueryTerm(term)
		if !exist {
			panic(term)
		}
		n, _ := json.Marshal(nodes)
		t.Log("从磁盘访问 ", term ,": ", string(n))

		term, _, ok = rIdx.btdb.GetNextKV(TEST_TREE, term)
	}
	t.Log("NextId:", rIdx.nextDocId)

	t.Log("\n\n")
}


//********************* 正排索引 *********************
func TestNewAndAddDoc(t *testing.T) {
	idx1 := NewEmptyForwardIndex(IDX_TYPE_INTEGER, 0) //数字型存入数字
	var err error
	err = idx1.AddDocument(0, 100); if err != nil {panic(err)}
	err = idx1.AddDocument(1, 200); if err != nil {panic(err)}

	iv, b := idx1.GetInt(0)
	if !b || iv != 100 {
		panic("Sth wrong")
	}
	t.Log("0: ", iv)

	iv, b = idx1.GetInt(3) //不存在
	if b {
		panic("Sth wrong")
	}

	idx2 := NewEmptyForwardIndex(IDX_TYPE_INTEGER, 0) //数字型存入时间戳
	err = idx2.AddDocument(0, 1556064868); if err != nil {panic(err)}
	iv, b = idx2.GetInt(0)
	if !b {
		panic("Sth wrong")
	}
	t.Log("0: ", iv)
	var sv string
	sv, b = idx2.GetTimeStr(0)
	if !b {
		panic("Sth wrong")
	}
	t.Log("0: ", sv)


	idx3 := NewEmptyForwardIndex(IDX_TYPE_STR_WHOLE, 0) //数字型存入字符
	err = idx3.AddDocument(0, "abc")
	if err != nil {
		panic(err)
	}
	idx3.AddDocument(1, "efg")
	if err != nil {
		panic(err)
	}
	sv, b = idx3.GetString(0)
	if !b || sv != "abc" {
		panic("Sth wrong")
	}
	t.Log("0: ", sv)

	sv, b = idx3.GetString(1)
	if !b || sv != "efg" {
		panic("Sth wrong")
	}
	t.Log("2: ", sv)


	idx4 := NewEmptyForwardIndex(IDX_TYPE_DATE, 0) //时间型存入字符串
	err = idx4.AddDocument(0, "2019-01-01 00:00:00"); if err != nil {panic(err)}
	iv, b = idx4.GetInt(0)
	if !b {
		panic("Sth wrong")
	}
	t.Log("0: ", iv)
	sv, b = idx4.GetTimeStr(0)
	if !b {
		panic("Sth wrong")
	}
	t.Log("0: ", sv)
	t.Log("\n\n")
}

func TestPersist(t *testing.T) {
	idx1 := NewEmptyForwardIndex(IDX_TYPE_INTEGER, 0) //数字型存入数字
	var err error
	err = idx1.AddDocument(0, 100); if err != nil {panic(err)}
	err = idx1.AddDocument(1, 200); if err != nil {panic(err)}
	err = idx1.AddDocument(2, 300); if err != nil {panic(err)}
	offset, cnt, err := idx1.Persist("/tmp/spider/Partition.int")
	if err != nil {
		panic(err)
	}
	t.Log("Persist ", "/tmp/spider/Partition.int.fwd. Offset:", offset, ". Cnt:", cnt)

	idx3 := NewEmptyForwardIndex(IDX_TYPE_STR_WHOLE, 0) //数字型存入字符
	err = idx3.AddDocument(0, "abc"); if err != nil {panic(err)}
	err = idx3.AddDocument(1, "efg"); if err != nil {panic(err)}
	offset, cnt, err = idx3.Persist("/tmp/spider/Partition.string")
	if err != nil {
		panic(err)
	}
	t.Log("Persist ", "/tmp/spider/Partition.string.fwd. Offset:", offset, ". Cnt:", cnt)

	t.Log("\n\n")
}

func TestLoadFwdIndex(t *testing.T) {
	mmp, err := mmap.NewMmap("/tmp/spider/Partition.int" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		panic(err)
	}
	idx1 := LoadForwardIndex(IDX_TYPE_INTEGER, mmp, nil, 0, 0, 0, 3)
	iv, b := idx1.GetInt(0)
	if !b || iv != 100 {
		panic("Sth wrong")
	}
	t.Log("0: ", iv)

	iv, b = idx1.GetInt(2)
	if !b || iv != 300 {
		panic("Sth wrong")
	}
	t.Log("2: ", iv)

	iv, b = idx1.GetInt(3) //不存在
	if b {
		panic("Sth wrong")
	}

	mmp1, err := mmap.NewMmap("/tmp/spider/Partition.string" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		panic(err)
	}
	mmp2, err := mmap.NewMmap("/tmp/spider/Partition.string" + basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		panic(err)
	}
	idx2 := LoadForwardIndex(IDX_TYPE_STR_WHOLE, mmp1, mmp2, 0, 0, 0, 2)

	sv, b := idx2.GetString(0)
	if !b || sv != "abc" {
		panic("Sth wrong")
	}
	t.Log("0: ", sv)

	sv, b = idx2.GetString(1)
	if !b || sv != "efg" {
		panic("Sth wrong")
	}
	t.Log("1: ", sv)

	t.Log("\n\n")
}

func TestMergeFwdIndex(t *testing.T) {
	idx1 := NewEmptyForwardIndex(IDX_TYPE_INTEGER, 0) //数字型存入数字
	if err := idx1.AddDocument(0, 100); err != nil {panic(err) }
	if err := idx1.AddDocument(1, 200); err != nil {panic(err) }
	if err := idx1.AddDocument(2, 300); err != nil {panic(err) }

	idx2 := NewEmptyForwardIndex(IDX_TYPE_INTEGER, 3) //数字型存入字符
	if err := idx2.AddDocument(3, 123); err != nil {panic(err) }
	if err := idx2.AddDocument(4, 456); err != nil {panic(err) }

	idx0 := NewEmptyForwardIndex(IDX_TYPE_INTEGER, 9999) //9999没用，会被覆盖
	_, _, err := idx0.MergePersistFwdIndex([]*ForwardIndex{idx1, idx2},"/tmp/spider/Partition.int.fwd.merge")
	if err != nil {
		panic(err)
	}
	if idx0.fwdOffset != 0 || idx0.docCnt != 5 || idx0.nextDocId != 5 {
		panic("Merge Error: wrong number")
	}
	mmp, err := mmap.NewMmap("/tmp/spider/Partition.int.fwd.merge"+basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		panic(err)
	}
	idx0.SetBaseMmap(mmp)
	t.Log("Merge ", "/tmp/spider/Partition.int.fwd.merge. Offset:", idx0.fwdOffset, ". Cnt:", idx0.docCnt, ". NextId:", idx0.nextDocId)
	//合并完毕立刻验证
	iv, b := idx0.GetInt(0)
	if !b || iv != 100 {
		panic(iv)
	}
	t.Log("0: ", iv)

	iv, b = idx0.GetInt(3)
	if !b || iv != 123 {
		panic(iv)
	}
	t.Log("3: ", iv)


	//Load回来验证
	idx := NewEmptyForwardIndex(IDX_TYPE_INTEGER, 0)
	idx = LoadForwardIndex(IDX_TYPE_INTEGER, mmp, nil, 0, 5, 0, 5)
	t.Log("Merge ", "/tmp/spider/Partition.int.fwd.merge. Offset:", idx.fwdOffset, ". Cnt:", idx.docCnt, ". NextId:", idx.nextDocId)
	iv, b = idx.GetInt(0)
	if !b || iv != 100 {
		panic(iv)
	}
	t.Log("0: ", iv)

	iv, b = idx.GetInt(3)
	if !b || iv != 123 {
		panic(iv)
	}
	t.Log("3: ", iv)


	t.Log("\n\n")
}

func TestMergeFwdIndexString(t *testing.T) {
	idx1 := NewEmptyForwardIndex(IDX_TYPE_STR_WHOLE, 0) //数字型存入字符
	idx1.AddDocument(0, "abc")
	idx1.AddDocument(1, "def")

	idx2 := NewEmptyForwardIndex(IDX_TYPE_STR_WHOLE, 2) //数字型存入字符
	idx2.AddDocument(2, "ghi")
	idx2.AddDocument(3, "jkl")

	idx0 := NewEmptyForwardIndex(IDX_TYPE_STR_WHOLE, 9999) //9999没用，会被覆盖

	_, _, err := idx0.MergePersistFwdIndex([]*ForwardIndex{idx1, idx2}, "/tmp/spider/Partition.string.fwd.merge")
	if err != nil {
		panic(err)
	}
	t.Log("Merge ", "/tmp/spider/Partition.string.fwd.merge. Offset:", idx0.fwdOffset, ". Cnt:", idx0.docCnt, ". NextId:", idx0.nextDocId)
	if idx0.fwdOffset != 0 || idx0.docCnt != 4 || idx0.nextDocId != 4 {
		panic("Merge Error: wrong number")
	}
	mmp1, err := mmap.NewMmap("/tmp/spider/Partition.string.fwd.merge" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		panic(err)
	}
	idx0.SetBaseMmap(mmp1)
	mmp2, err := mmap.NewMmap("/tmp/spider/Partition.string.fwd.merge" + basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		panic(err)
	}
	idx0.SetExtMmap(mmp2)

	//合并完毕直接验证
	iv, b := idx0.GetString(0)
	if !b || iv != "abc" {
		panic(iv)
	}
	t.Log("0: ", iv)

	iv, b = idx0.GetString(3)
	if !b || iv != "jkl" {
		panic(iv)
	}
	t.Log("3: ", iv)

	//Load回来验证
	idx := LoadForwardIndex(IDX_TYPE_STR_WHOLE, mmp1, mmp2, 0, 4, 0, 4)
	iv, b = idx.GetString(0)
	if !b || iv != "abc" {
		panic(iv)
	}
	t.Log("0: ", iv)

	iv, b = idx.GetString(3)
	if !b || iv != "jkl" {
		panic(iv)
	}
	t.Log("3: ", iv)
	t.Log("\n\n")
}
