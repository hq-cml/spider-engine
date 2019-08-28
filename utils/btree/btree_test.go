package btree

import "testing"

func TestBtree(t *testing.T) {
	treeName := "Test"
	btree := NewBtree("", "/tmp/btree")
	btree.AddTree(treeName)
	btree.Set(treeName, "a", "b")
	v, e := btree.GetStr(treeName, "a")
	if !e {
		t.Error("not exist")
	}
	if v != "b" {
		t.Error("Should is b")
	}
}
