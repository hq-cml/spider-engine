package boltbtree

/*
 * bolt封装
 */
import (
	"fmt"
	"os"
	"time"
	"sync"
	"errors"
	"encoding/json"
	"github.com/boltdb/bolt"
	"github.com/hq-cml/spider-engine/utils/log"
)

//BoltWrapper
type BoltWrapper struct {
	mutex    sync.RWMutex    //保护Tables
	fileName string
	db       *bolt.DB
	Tables   map[string]bool
}

//单例
var gBoltWrapper *BoltWrapper = nil

//初始化单例Btree
func InitBoltWrapper(dbname string, mode os.FileMode, timeout time.Duration) error {
	var err error
	gBoltWrapper, err = NewBoltWrapper(dbname, mode, timeout)
	if err != nil {
		return err
	}
	return nil
}

//New
func NewBoltWrapper(fineName string, mode os.FileMode, timeout time.Duration) (*BoltWrapper, error) {
	var err error
	wrapper := &BoltWrapper {
		fileName:    fineName,
		Tables: map[string]bool{},
	}
	wrapper.db, err = bolt.Open(fineName, mode, &bolt.Options{Timeout: timeout})
	if err != nil {
		log.Errf("Open Dbname Error: %v. Filename: %s\n", err, fineName)
		return nil, err
	}

	//初始化填充Tables
	wrapper.db.View(func(tx *bolt.Tx) error {
		tx.ForEach(func(k []byte, v *bolt.Bucket) error {
			wrapper.Tables[string(k)] = true
			return nil
		})
		return nil
	})

	return wrapper, nil
}

//获取单例
func GetBoltWrapperInstance() *BoltWrapper {
	if gBoltWrapper == nil {
		InitBoltWrapper("/tmp/spider.db", 0666, 3 * time.Second)
	}
	return gBoltWrapper
}

//新建表 (在Bolt底层其实就是新建一个bucket)
func (br *BoltWrapper) CreateTable(tableName string) error {

	if err := br.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(tableName)); err != nil {
			return err
		}
		br.mutex.Lock()
		defer br.mutex.Unlock()
		br.Tables[tableName] = true;
		return nil
	}); err != nil {
		log.Fatal("CreateTable Error:", err)
		return err
	}

	return nil
}

//删除表 (在Bolt底层其实就是新建一个bucket)
func (br *BoltWrapper) DeleteTable(tableName string) error {

	if err := br.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte(tableName)); err != nil {
			return err
		}
		br.mutex.Lock()
		defer br.mutex.Unlock()
		delete(br.Tables, tableName);
		return nil
	}); err != nil {
		log.Fatal("DeleteTable Error:", err)
		return err
	}
	return nil
}

//更新
func (br *BoltWrapper) Set(tableName, key, value string) error {
	if err := br.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return errors.New(fmt.Sprintf("Tablename[%v] not found", tableName))
		}
		err := b.Put([]byte(key), []byte(value))
		return err
	}); err != nil {
		log.Errln("Update Error:", err)
		return err
	}

	return nil
}

//批量更新
func (br *BoltWrapper) MutiSet(tableName string, kv map[string]string) error {

	if err := br.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return errors.New(fmt.Sprintf("Tablename[%v] not found", tableName))
		}
		for k, v := range kv {
			if err := b.Put([]byte(k), []byte(v)); err != nil {
				return err
			}

		}
		return nil
	}); err != nil {
		log.Errln("MutiUpdate Error:", err)
		return err
	}
	return nil
}

//更新一个对象(以 json 形式)
func (br *BoltWrapper) SetObj(tableName, key string, obj interface{}) error {

	value, err := json.Marshal(obj)
	if err != nil {
		log.Errln("json.Marshal Error:%v", err)
		return err
	}

	if err = br.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return errors.New(fmt.Sprintf("Tablename[%v] not found", tableName))
		}
		err := b.Put([]byte(key), value)
		return err
	}); err != nil {
		log.Errln("SetObj Error:", err)
		return err
	}

	return nil
}

//Get string
func (br *BoltWrapper) Get(tableName, key string) (string, bool) {

	var value []byte
	if err := br.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return errors.New(fmt.Sprintf("Tablename[%v] not found", tableName))
		}
		value = b.Get([]byte(key))
		return nil
	}); err != nil {
		log.Errln("Get Error:", err)
		return "", false
	}

	if value == nil {
		return "", false
	}

	return string(value), true
}

//Get object
func (br *BoltWrapper) GetValue(tableName, key string) ([]byte, bool) {

	var value []byte
	if err := br.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return errors.New(fmt.Sprintf("Tablename[%v] not found", tableName))
		}
		value = b.Get([]byte(key))
		return nil
	}); err != nil {
		log.Errln("Get Error:", err)
		return nil, false
	}

	if value == nil {
		return nil, false
	}

	return value, true
}

//HasKey
func (this *BoltWrapper) HasKey(tableName, key string) bool {
	_, ok := this.Get(tableName, key)
	return ok
}

//Next
func (br *BoltWrapper) GetNextKV(tableName, key string) (string, string, error) {
	var k []byte
	var v []byte
	if err := br.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return errors.New(fmt.Sprintf("Tablename[%v] not found", tableName))
		}
		c := b.Cursor()
		c.Seek([]byte(key))
		k, v = c.Next()
		return nil
	}); err != nil {
		log.Errln("GetNextKV Error:", err)
		return "", "", err
	}

	if k == nil || v == nil {
		return "", "", fmt.Errorf("Key[%v] Not Found", key)
	}

	return string(k), string(v), nil

}

//First
func (br *BoltWrapper) GetFristKV(tableName string) (string, string, error) {

	var k []byte
	var v []byte
	if err := br.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return errors.New(fmt.Sprintf("Tablename[%v] not found", tableName))
		}
		c := b.Cursor()
		k, v = c.First()
		return nil
	}); err != nil {
		log.Errln("GetNextKV Error:", err)
		return "", "", err
	}

	if k == nil || v == nil {
		return "", "", fmt.Errorf("Not Found")
	}

	return string(k), string(v), nil
}

//Close
func (br *BoltWrapper) CloseDB() error {
	return br.db.Close()
}

//print all k,v
func (br *BoltWrapper) DisplayTable(tableName string) error {
	if err := br.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return errors.New(fmt.Sprintf("Tablename[%v] not found", tableName))
		}
		b.ForEach(func(k, v []byte) error {
			fmt.Printf("key=%s, value=%s\n", k, v)
			return nil
		})
		return nil
	}); err != nil {
		log.Errln("DisplayTable Error:", err)
		return err
	}

	return nil

}
