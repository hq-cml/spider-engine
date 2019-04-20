package database

import "testing"

func TestNewDatabase(t *testing.T) {
	db, err := NewDatabase("/tmp/spider", "spider")
	if err != nil {
		t.Fatal(err)
	}

	_ = db
}