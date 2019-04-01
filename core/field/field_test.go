package field

import (
	"testing"
	"os/exec"
	"os"
	"github.com/hq-cml/spider-engine/core/index"
	"fmt"
	"github.com/hq-cml/spider-engine/utils/json"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/basic"
)

const TEST_FIELD = "user_name"

func init() {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -f /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
}

func TestAddDocAndQueryAndGetAndPersist(t *testing.T) {
	field := NewEmptyField(TEST_FIELD, 0, index.IDX_TYPE_STRING_SEG)

	//add doc
	field.AddDocument(0, "我爱北京天安门")
	field.AddDocument(1, "天安门上太阳升")
	field.AddDocument(2, "火红的太阳")

	//测试query
	tmp, b := field.Query("天安门")
	if !b {
		t.Fatal("Wrong")
	}
	fmt.Println(json.JsonEnocde(tmp))
	if len(tmp) != 2 {
		t.Fatal("Wrong")
	}

	//测试get
	s, b := field.GetString(2)
	if !b {
		t.Fatal("Wrong")
	}
	if s != "火红的太阳" {
		t.Fatal("Wrong")
	}

	_, b = field.GetString(3)
	if b {
		t.Fatal("Wrong")
	}

	//准备落地
	t.Logf("FileOffset: %v, DocCnt: %v", field.fwdOffset, field.fwdDocCnt)
	treedb := btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer treedb.Close()
	if err := field.Persist("/tmp/spider/Segment0", treedb); err != nil {
		t.Fatal("Wrong:", err)
	}

	t.Logf("FileOffset: %v, DocCnt: %v", field.fwdOffset, field.fwdDocCnt)
}

//func TestLoad(t *testing.T) {
//	field := LoadField(TEST_FIELD, )
//}