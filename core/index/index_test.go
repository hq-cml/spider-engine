package index

import (
	"os"
	"os/exec"
	"testing"
	"encoding/json"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/btree"
	//"github.com/hq-cml/spider-engine/utils/mmap"
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
	t.Skip()
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

func TestAddDoc(t *testing.T) {
	rIdx := NewReverseIndex(IDX_TYPE_STRING_SEG, 0, TEST_TREE)
	rIdx.addDocument(0, "我爱北京天安门")
	rIdx.addDocument(1, "天安门上太阳升")
	rIdx.addDocument(2, "火红的太阳")
	r, _ := json.Marshal(rIdx.termMap)
	t.Log(string(r))
	t.Log("\n\n")
}

func TestQureyTermInMemAndPersist(t *testing.T) {
	rIdx := NewReverseIndex(IDX_TYPE_STRING_SEG, 0, TEST_TREE)
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
	rIdx := NewReverseIndex(IDX_TYPE_STRING_SEG, 0, TEST_TREE)
	rIdx.isMomery = false //写死, 强制走磁盘

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
