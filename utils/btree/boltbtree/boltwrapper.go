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
	mutex    sync.RWMutex    //保护Buckets
	fileName string
	db       *bolt.DB
	Buckets  map[string]bool
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
		fileName: fineName,
		Buckets:  map[string]bool{},
	}
	wrapper.db, err = bolt.Open(fineName, mode, &bolt.Options{Timeout: timeout})
	if err != nil {
		log.Errf("Open Dbname Error: %v. Filename: %s\n", err, fineName)
		return nil, err
	}

	//初始化填充Buckets
	wrapper.db.View(func(tx *bolt.Tx) error {
		tx.ForEach(func(k []byte, v *bolt.Bucket) error {
			wrapper.Buckets[string(k)] = true
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
func (br *BoltWrapper) CreateBucket(bucketName string) error {

	if err := br.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(bucketName)); err != nil {
			return err
		}
		br.mutex.Lock()
		defer br.mutex.Unlock()
		br.Buckets[bucketName] = true;
		return nil
	}); err != nil {
		log.Fatal("CreateBucket Error:", err)
		return err
	}

	return nil
}

//删除表 (在Bolt底层其实就是新建一个bucket)
func (br *BoltWrapper) DeleteBucket(bucketName string) error {

	if err := br.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte(bucketName)); err != nil {
			return err
		}
		br.mutex.Lock()
		defer br.mutex.Unlock()
		delete(br.Buckets, bucketName);
		return nil
	}); err != nil {
		log.Fatal("DeleteBucket Error:", err)
		return err
	}
	return nil
}

//更新
func (br *BoltWrapper) Set(bucketName, key, value string) error {
	if err := br.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.New(fmt.Sprintf("Bucketname[%v] not found", bucketName))
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
func (br *BoltWrapper) MutiSet(bucketName string, kv map[string]string) error {

	if err := br.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.New(fmt.Sprintf("Bucketname[%v] not found", bucketName))
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
func (br *BoltWrapper) SetObj(bucketName, key string, obj interface{}) error {

	value, err := json.Marshal(obj)
	if err != nil {
		log.Errln("json.Marshal Error:%v", err)
		return err
	}

	if err = br.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.New(fmt.Sprintf("Bucketname[%v] not found", bucketName))
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
func (br *BoltWrapper) Get(bucketName, key string) (string, bool) {
	var value []byte
	if err := br.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.New(fmt.Sprintf("Bucketname[%v] not found", bucketName))
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
func (br *BoltWrapper) GetValue(bucketName, key string) ([]byte, bool) {

	var value []byte
	if err := br.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.New(fmt.Sprintf("Bucketname[%v] not found", bucketName))
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
func (br *BoltWrapper) HasKey(bucketName, key string) bool {
	_, ok := br.Get(bucketName, key)
	return ok
}

//Next
func (br *BoltWrapper) GetNextKV(bucketName, key string) (string, string, error) {
	var k []byte
	var v []byte
	if err := br.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.New(fmt.Sprintf("Bucketname[%v] not found", bucketName))
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
func (br *BoltWrapper) GetFristKV(bucketName string) (string, string, error) {

	var k []byte
	var v []byte
	if err := br.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.New(fmt.Sprintf("Bucketname[%v] not found", bucketName))
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
func (br *BoltWrapper) DisplayBucket(bucketName string) error {
	if err := br.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.New(fmt.Sprintf("Bucketname[%v] not found", bucketName))
		}
		b.ForEach(func(k, v []byte) error {
			fmt.Printf("key=%s, value=%s\n", k, v)
			return nil
		})
		return nil
	}); err != nil {
		log.Errln("DisplayBucket Error:", err)
		return err
	}

	return nil

}

//print all k,v
func (br *BoltWrapper) HasBucket(bucketName string) bool {
	_, exist := br.Buckets[bucketName]
	return exist

}
