package engine
/*
 * 封装了ddl操作
 */
import (
	"github.com/hq-cml/spider-engine/utils/log"
	"fmt"
	"github.com/hq-cml/spider-engine/core/database"
	"errors"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/core/index"
)

//建库
func (se *SpiderEngine) CreateDatabase(p *DatabaseParam) error {
	if se.Closed {
		return errors.New("Spider Engine is closed!")
	}
	se.RwMutex.Lock()
	defer se.RwMutex.Unlock()

	//校验
	_, exist := se.DbMap[p.Database]
	if exist {
		log.Errf("The db already exist!")
		return errors.New("The db already exist!")
	}

	//创建表和字段
	path := fmt.Sprintf("%s%s", se.Path, p.Database)
	db, err := database.NewDatabase(path, p.Database)
	if err != nil {
		log.Errf("CreateDatabase Error: %v, %v", err, path)
		return err
	}

	//关联进入db
	se.DbMap[p.Database] = db
	se.DbList = append(se.DbList, p.Database)

	//meta落地
	err = se.storeMeta()
	if err != nil {
		log.Errf("storeMeta Error: %v", err)
		return err
	}

	log.Infof("Create database: %v", p.Database)
	return nil
}

//删库
func (se *SpiderEngine) DropDatabase(p *DatabaseParam) error {
	if se.Closed {
		return errors.New("Spider Engine is closed!")
	}
	se.RwMutex.Lock()
	defer se.RwMutex.Unlock()
	//校验
	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return errors.New("The db not exist!")
	}

	//删除库
	err := db.Destory()
	if err != nil {
		log.Errf("DropDatabase Error: %v", err)
		return err
	}

	//删slice
	delete(se.DbMap, p.Database)
	for i := 0; i < len(se.DbList); i++ {
		if se.DbList[i] == p.Database {
			se.DbList = append(se.DbList[:i], se.DbList[i+1:]...)
		}
	}

	//更新meta
	err = se.storeMeta()
	if err != nil {
		log.Errf("storeMeta Error: %v", err)
		return err
	}

	log.Infof("DropDatabase database: %v", p.Database)
	return nil
}

//建表
func (se *SpiderEngine) CreateTable(p *CreateTableParam) error {
	if se.Closed {
		return errors.New("Spider Engine is closed!")
	}
	se.RwMutex.Lock()
	defer se.RwMutex.Unlock()

	//校验
	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return errors.New("The db not exist!")
	}

	//参数拼装
	fields := []field.BasicField{}
	for _, f := range p.Fileds {
		t, ok := index.IDX_MAP[f.Type]
		if !ok {
			log.Errf("Unsuport index type: %v", f.Type)
			return errors.New("Unsuport index type: " + f.Type)
		}
		fields = append(fields, field.BasicField{
			FieldName:  f.Name,
			IndexType:  t,
		})
	}

	//建表
	_, err := db.CreateTable(p.Table, fields)
	if err != nil {
		log.Errf("CreateTable Error: %v", err)
		return err
	}

	log.Infof("Create Table: %v", p.Database + "." + p.Table)
	return nil
}

//建表
func (se *SpiderEngine) DropTable(p *CreateTableParam) error {
	if se.Closed {
		return errors.New("Spider Engine is closed!")
	}
	se.RwMutex.Lock()
	defer se.RwMutex.Unlock()

	//校验
	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return errors.New("The db not exist!")
	}

	//删表
	err := db.DropTable(p.Table)
	if err != nil {
		log.Errf("Drop Table Error: %v", err)
		return err
	}

	log.Infof("Drop Table: %v", p.Database + "." + p.Table)
	return nil
}

//增字段
func (se *SpiderEngine) AddField(p *AddFieldParam) error {
	if se.Closed {
		return errors.New("Spider Engine is closed!")
	}
	se.RwMutex.RLock()          //读锁
	defer se.RwMutex.RUnlock()

	//校验
	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return errors.New("The db not exist!")
	}
	t, ok := index.IDX_MAP[p.Filed.Type]
	if !ok {
		log.Errf("Unsuport index type: %v", p.Filed.Type)
		return errors.New(fmt.Sprintf("Unsuport index type: %v", p.Filed.Type))
	}

	//新增
	fld := field.BasicField{
		FieldName: p.Filed.Name,
		IndexType: t,
	}
	err := db.AddField(p.Table, fld)
	if err != nil {
		log.Errf("AddField Error: %v", err)
		return err
	}

	log.Infof("Add Field: %v", p.Database + "." + p.Table + "." + p.Filed.Name + "." + p.Filed.Type)
	return nil
}

//减字段
func (se *SpiderEngine) DeleteField(p *AddFieldParam) error {
	if se.Closed {
		return errors.New("Spider Engine is closed!")
	}
	se.RwMutex.RLock()          //读锁
	defer se.RwMutex.RUnlock()

	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return errors.New("The db not exist!")
	}

	err := db.DeleteField(p.Table, p.Filed.Name)
	if err != nil {
		log.Errf("DeleteField Error: %v", err)
		return err
	}

	log.Infof("Delete Field: %v", p.Database + "." + p.Table + "." + p.Filed.Name)
	return nil
}













