package search

/*
func TestFilterNums(t *testing.T) {
	idx1 := NewEmptyForwardIndex(IDX_TYPE_NUMBER, 0) //数字型存入数字
	if err := idx1.AddDocument(0, 100); err != nil {t.Fatal("add Error:", err) }
	if err := idx1.AddDocument(1, 200); err != nil {t.Fatal("add Error:", err) }
	if err := idx1.AddDocument(2, 300); err != nil {t.Fatal("add Error:", err) }
	if err := idx1.AddDocument(3, 400); err != nil {t.Fatal("add Error:", err) }
	if err := idx1.AddDocument(4, 500); err != nil {t.Fatal("add Error:", err) }

	if !idx1.FilterNums(1, basic.FILT_EQ, []int64{300, 200}) {
		t.Fatal("Sth wrong")
	}
	if idx1.FilterNums(1, basic.FILT_EQ, []int64{300, 400}) {
		t.Fatal("Sth wrong")
	}
	t.Log("\n\n")
}
*/