package database

import (
	"testing"
	"os/exec"
	"os"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/core/index"
)

const TEST_TABLE = "user"         //用户
const TEST_FIELD0 = "user_id"
const TEST_FIELD1 = "user_name"
const TEST_FIELD2 = "user_age"
const TEST_FIELD3 = "user_desc"
const TEST_FIELD4 = "tobe_del"

func init() {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
}

func TestNewDatabase(t *testing.T) {
	db, err := NewDatabase("/tmp/spider", "db1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.CreateTable(TEST_TABLE, []field.CoreField{
		{
			FieldName: TEST_FIELD0,
			IndexType: index.IDX_TYPE_PK,
		},
		{
			FieldName: TEST_FIELD1,
			IndexType: index.IDX_TYPE_STRING,
		},
		{
			FieldName: TEST_FIELD2,
			IndexType: index.IDX_TYPE_NUMBER,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}