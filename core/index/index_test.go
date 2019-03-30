package index

import (
	"os"
	"os/exec"
	"testing"
	"encoding/json"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/utils/mmap"
)

const TEST_TREE = "user_name"

func TestInit(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -f /tmp/spider/*`)
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
	rIdx := NewInvertedIndex(IDX_TYPE_STRING_SEG, 0, TEST_TREE)
	rIdx.addDocument(0, "我爱北京天安门")
	rIdx.addDocument(1, "天安门上太阳升")
	rIdx.addDocument(2, "火红的太阳")
	r, _ := json.Marshal(rIdx.termMap)
	t.Log(string(r))
	t.Log("\n\n")
}

func TestQureyTermInMemAndPersist(t *testing.T) {
	rIdx := NewInvertedIndex(IDX_TYPE_STRING_SEG, 0, TEST_TREE)
	rIdx.addDocument(0, "我爱北京天安门")
	rIdx.addDocument(1, "天安门上太阳升")
	rIdx.addDocument(2, "火红的太阳")
	r, _ := json.Marshal(rIdx.termMap)
	t.Log(string(r))

	//从内存读取
	nodes, exist := rIdx.queryTerm("天安门")
	if !exist {
		t.Fatal("Wrong exist")
	}
	n, _ := json.Marshal(nodes)
	t.Log("从内存访问: ", string(n))
	if len(nodes)!=2 {
		t.Fatal("Wrong somthing")
	}

	//测试落地
	tree := btree.NewBtree("xx", "/tmp/spider/spider.db")
	defer tree.Close()
	tree.AddBTree(TEST_TREE)
	rIdx.persist("/tmp/spider/Segment0", tree)
	t.Log("\n\n")
}

func TestQureyTermInFile(t *testing.T) {
	//新建索引
	rIdx := NewInvertedIndex(IDX_TYPE_STRING_SEG, 0, TEST_TREE)
	rIdx.isMemory = false //写死, 强制走磁盘

	//从磁盘加载btree
	//InitBoltWrapper("/tmp/spider/spider.db", 0666, 3 * time.Second)
	rIdx.btree = btree.NewBtree("xx", "/tmp/spider/spider.db")
	defer rIdx.btree.Close()
	//从磁盘加载mmap
	var err error
	rIdx.idxMmap, err = mmap.NewMmap("/tmp/spider/Segment0.idx", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	nodes, exist := rIdx.queryTerm("天安门")
	if !exist {
		t.Fatal("Wrong exist")
	}
	n, _ := json.Marshal(nodes)
	t.Log("从磁盘访问 天安门: ", string(n))


	nodes, exist = rIdx.queryTerm("太阳")
	if !exist {
		t.Fatal("Wrong exist")
	}
	n, _ = json.Marshal(nodes)
	t.Log("从磁盘访问 太阳: ", string(n))

	t.Log("\n\n")
}

func TestMergeIndex(t *testing.T) {
	//清空目录
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -f /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}

	//建一颗B+树 => 建立索引1 => 落地索引1 => 再加载索引1
	tree1 := btree.NewBtree("xx", "/tmp/spider/spider_1.db")
	defer tree1.Close()
	tree1.AddBTree(TEST_TREE)
	rIdx1 := NewInvertedIndex(IDX_TYPE_STRING_LIST, 1, TEST_TREE)
	rIdx1.addDocument(1, "c;f")
	rIdx1.addDocument(2, "a;c")
	rIdx1.addDocument(3, "f;a")
	r, _ := json.Marshal(rIdx1.termMap)
	rIdx1.persist("/tmp/spider/Segment_1", tree1) //落地
	t.Log(string(r))
	rIdx1.idxMmap, err = mmap.NewMmap("/tmp/spider/Segment_1.idx", true, 0) //加载
	if err != nil {
		t.Fatal(err)
	}

	//建一颗B+树 => 建立索引2 => 落地索引2 => 再加载索引2
	tree2 := btree.NewBtree("xx", "/tmp/spider/spider_2.db")
	defer tree2.Close()
	tree2.AddBTree(TEST_TREE)
	rIdx2 := NewInvertedIndex(IDX_TYPE_STRING_LIST, 4, TEST_TREE)
	rIdx2.addDocument(4, "b;d")
	rIdx2.addDocument(5, "d;c")
	rIdx2.addDocument(6, "b;c")
	r, _ = json.Marshal(rIdx2.termMap)
	rIdx2.persist("/tmp/spider/Segment_2", tree2) //落地
	t.Log(string(r))
	rIdx2.idxMmap, err = mmap.NewMmap("/tmp/spider/Segment_2.idx", true, 0) //加载
	if err != nil {
		t.Fatal(err)
	}

	//建一颗B+树 => 建立索引3 => 落地索引3 => 再加载索引3
	tree3 := btree.NewBtree("xx", "/tmp/spider/spider_3.db")
	defer tree3.Close()
	tree3.AddBTree(TEST_TREE)
	rIdx3 := NewInvertedIndex(IDX_TYPE_STRING_LIST, 7, TEST_TREE)
	rIdx3.addDocument(7, "c;e")
	rIdx3.addDocument(8, "a;e")
	rIdx3.addDocument(9, "c;a")
	r, _ = json.Marshal(rIdx3.termMap)
	rIdx3.persist("/tmp/spider/Segment_3", tree3) //落地
	t.Log(string(r))
	rIdx3.idxMmap, err = mmap.NewMmap("/tmp/spider/Segment_3.idx", true, 0) //加载
	if err != nil {
		t.Fatal(err)
	}

	//合并 => 加载回来 => 读一下试一下
	tree := btree.NewBtree("xx", "/tmp/spider/spider.db")
	defer tree.Close()
	tree.AddBTree(TEST_TREE)
	rIdx := NewInvertedIndex(IDX_TYPE_STRING_LIST, 0, TEST_TREE)
	rIdx.mergeIndex(
		[]*InvertedIndex{rIdx1, rIdx2, rIdx3}, "/tmp/spider/Segment", tree)

	rIdx.idxMmap, err = mmap.NewMmap("/tmp/spider/Segment.idx", true, 0)
	if err != nil {
		t.Fatal(err)
	}

	term, _, _, _, ok := rIdx.btree.GetFristKV(TEST_TREE)
	for ok {
		nodes, exist := rIdx.queryTerm(term)
		if !exist {
			t.Fatal("Wrong exist")
		}
		n, _ := json.Marshal(nodes)
		t.Log("从磁盘访问 ", term ,": ", string(n))

		term, _, _, _, ok = rIdx.btree.GetNextKV(TEST_TREE, term)
	}

	t.Log("\n\n")
}


//********************* 正排索引 *********************
func TestNewAndAddDoc(t *testing.T) {
	idx1 := newEmptyProfile(IDX_TYPE_NUMBER, 0)  //数字型存入数字
	idx1.addDocument(0, 100)
	idx1.addDocument(1, 200)
	idx1.addDocument(2, 300)

	iv, b := idx1.getInt(0)
	if !b || iv != 100 {
		t.Fatal("Sth wrong")
	}
	t.Log("0: ", iv)

	iv, b = idx1.getInt(2)
	if !b || iv != 300 {
		t.Fatal("Sth wrong")
	}
	t.Log("2: ", iv)

	iv, b = idx1.getInt(3) //不存在
	if b {
		t.Fatal("Sth wrong")
	}

	idx2 := newEmptyProfile(IDX_TYPE_NUMBER, 0) //数字型存入字符
	idx2.addDocument(0, "123")
	idx2.addDocument(1, "456")
	iv, b = idx2.getInt(0)
	if !b || iv != 123 {
		t.Fatal("Sth wrong")
	}
	t.Log("0: ", iv)

	var sv string
	sv, b = idx2.getString(1)
	if !b || sv != "456" {
		t.Fatal("Sth wrong")
	}
	t.Log("2: ", sv)


	idx3 := newEmptyProfile(IDX_TYPE_STRING, 0) //数字型存入字符
	err := idx3.addDocument(0, "abc")
	if err != nil {
		t.Fatal("addDocument Error:", err)
	}
	idx3.addDocument(1, "efg")
	if err != nil {
		t.Fatal("addDocument Error:", err)
	}
	sv, b = idx3.getString(0)
	if !b || sv != "abc" {
		t.Fatal("Sth wrong")
	}
	t.Log("0: ", sv)

	sv, b = idx3.getString(1)
	if !b || sv != "efg" {
		t.Fatal("Sth wrong")
	}
	t.Log("2: ", sv)

	t.Log("\n\n")
}

func TestPersist(t *testing.T) {
	idx1 := newEmptyProfile(IDX_TYPE_NUMBER, 0)  //数字型存入数字
	idx1.addDocument(0, 100)
	idx1.addDocument(1, 200)
	idx1.addDocument(2, 300)
	offset, cnt, err := idx1.persist("/tmp/spider/Segment.int.fwd")
	if err != nil {
		t.Fatal("Persist Error:", err)
	}
	t.Log("Persist ", "/tmp/spider/Segment.int.fwd. Offset:", offset, ". Cnt:", cnt)

	idx3 := newEmptyProfile(IDX_TYPE_STRING, 0) //数字型存入字符
	idx3.addDocument(0, "abc")
	idx3.addDocument(1, "efg")
	offset, cnt, err = idx3.persist("/tmp/spider/Segment.string.fwd")
	if err != nil {
		t.Fatal("Persist Error:", err)
	}
	t.Log("Persist ", "/tmp/spider/Segment.string.fwd. Offset:", offset, ". Cnt:", cnt)
}

func TestLoadFwdIndex(t *testing.T) {
	mmp, err := mmap.NewMmap("/tmp/spider/Segment.int.fwd.pfl", true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}
	idx1 := newProfileWithLocalFile(IDX_TYPE_NUMBER,
		mmp, nil, 0, 0, false)
	iv, b := idx1.getInt(0)
	if !b || iv != 100 {
		t.Fatal("Sth wrong")
	}
	t.Log("0: ", iv)

	iv, b = idx1.getInt(2)
	if !b || iv != 300 {
		t.Fatal("Sth wrong")
	}
	t.Log("2: ", iv)

	iv, b = idx1.getInt(3) //不存在
	if b {
		t.Fatal("Sth wrong")
	}

	mmp1, err := mmap.NewMmap("/tmp/spider/Segment.string.fwd.pfl", true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}
	mmp2, err := mmap.NewMmap("/tmp/spider/Segment.string.fwd.dtl", true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}
	idx2 := newProfileWithLocalFile(IDX_TYPE_STRING,
		mmp1, mmp2, 0, 0, false)

	sv, b := idx2.getString(0)
	if !b || sv != "abc" {
		t.Fatal("Sth wrong")
	}
	t.Log("0: ", sv)

	sv, b = idx2.getString(1)
	if !b || sv != "efg" {
		t.Fatal("Sth wrong")
	}
	t.Log("1: ", sv)
}

func TestMergeFwdIndex(t *testing.T) {
	idx1 := newEmptyProfile(IDX_TYPE_NUMBER, 0)  //数字型存入数字
	if err := idx1.addDocument(0, 100); err != nil {t.Fatal("add Error:", err) }
	if err := idx1.addDocument(1, 200); err != nil {t.Fatal("add Error:", err) }
	if err := idx1.addDocument(2, 300); err != nil {t.Fatal("add Error:", err) }

	idx2 := newEmptyProfile(IDX_TYPE_NUMBER, 0) //数字型存入字符
	if err := idx2.addDocument(0, "123"); err != nil {t.Fatal("add Error:", err) }
	if err := idx2.addDocument(1, "456"); err != nil {t.Fatal("add Error:", err) }

	idx := newEmptyProfile(IDX_TYPE_NUMBER, 0)
	//TODO 这个地方存在坑, 如果idx1, idx2的顺序不对,就会出坑
	offset, cnt, err := idx.mergeIndex([]*ForwardIndex{idx1, idx2},"/tmp/spider/Segment.int.fwd.merge")
	if err != nil {
		t.Fatal("Merge Error:", err)
	}
	t.Log("Merge ", "/tmp/spider/Segment.int.fwd.merge Offset:", offset, ". Cnt:", cnt)

	//Load回来验证
	mmp, err := mmap.NewMmap("/tmp/spider/Segment.int.fwd.merge.pfl", true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}
	idx = newProfileWithLocalFile(IDX_TYPE_NUMBER,
		mmp, nil, 0, 0, false)
	iv, b := idx.getInt(0)
	if !b || iv != 100 {
		t.Fatal("Sth wrong", iv)
	}
	t.Log("0: ", iv)

	iv, b = idx.getInt(3)
	if !b || iv != 123 {
		t.Fatal("Sth wrong", iv)
	}
	t.Log("3: ", iv)
}

func TestMergeFwdIndexString(t *testing.T) {
	idx1 := newEmptyProfile(IDX_TYPE_STRING, 0) //数字型存入字符
	idx1.addDocument(0, "abc")
	idx1.addDocument(1, "def")

	idx2 := newEmptyProfile(IDX_TYPE_STRING, 0) //数字型存入字符
	idx2.addDocument(0, "ghi")
	idx2.addDocument(1, "jkl")

	idx := newEmptyProfile(IDX_TYPE_STRING, 0)
	//TODO 这个地方存在坑, 如果idx1, idx2的顺序不对,就会出坑
	offset, cnt, err := idx.mergeIndex([]*ForwardIndex{idx1, idx2}, "/tmp/spider/Segment.int.fwd.merge.string")
	if err != nil {
		t.Fatal("Merge Error:", err)
	}
	t.Log("Merge ", "/tmp/spider/Segment.int.fwd.merge.string Offset:", offset, ". Cnt:", cnt)

	//Load回来验证
	mmp1, err := mmap.NewMmap("/tmp/spider/Segment.int.fwd.merge.string.pfl", true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}
	mmp2, err := mmap.NewMmap("/tmp/spider/Segment.int.fwd.merge.string.dtl", true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}
	idx = newProfileWithLocalFile(IDX_TYPE_STRING,
		mmp1, mmp2, 0, 0, false)
	iv, b := idx.getString(0)
	if !b || iv != "abc" {
		t.Fatal("Sth wrong", iv)
	}
	t.Log("0: ", iv)

	iv, b = idx.getString(3)
	if !b || iv != "jkl" {
		t.Fatal("Sth wrong", iv)
	}
	t.Log("3: ", iv)
}

func TestFilterNums(t *testing.T) {
	idx1 := newEmptyProfile(IDX_TYPE_NUMBER, 0)  //数字型存入数字
	if err := idx1.addDocument(0, 100); err != nil {t.Fatal("add Error:", err) }
	if err := idx1.addDocument(1, 200); err != nil {t.Fatal("add Error:", err) }
	if err := idx1.addDocument(2, 300); err != nil {t.Fatal("add Error:", err) }
	if err := idx1.addDocument(3, 400); err != nil {t.Fatal("add Error:", err) }
	if err := idx1.addDocument(4, 500); err != nil {t.Fatal("add Error:", err) }

	if !idx1.filterNums(1, basic.FILT_EQ, []int64{300, 200}) {
		t.Fatal("Sth wrong")
	}
	if idx1.filterNums(1, basic.FILT_EQ, []int64{300, 400}) {
		t.Fatal("Sth wrong")
	}
}