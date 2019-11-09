package helper

import "testing"

func TestFileOp(t *testing.T) {
	err := OverWriteToFile([]byte("Hello world"), "/tmp/tmpFile")
	if err != nil {
		t.Error("Write err:", err)
	}

	exist := Exist("/tmp/tmpFile")
	if !exist {
		t.Error("Not Exist")
	}

	data, err := ReadFile("/tmp/tmpFile")
	if err != nil {
		t.Error("Read err:", err)
	}
	if string(data) != "Hello world" {
		t.Error("Not same")
	}
}

func TestTime(t *testing.T) {
	tt, err := String2Timestamp("2019-10-10 00:01:01")
	if err != nil {
		t.Error("String2Timestamp err:", err)
	}
	t.Log(tt)

	s := Timestamp2String(tt)
	if s != "2019-10-10 00:01:01" {
		t.Error("not same")
	}
}