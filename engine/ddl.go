package engine
/*
 * 封装了ddl操作
 * Note：
 *  其中
 *    建库、删库、建表、删表等操作，相对低频，且需要建立调度等附加工作，故采用直接执行的方式
 *    增减字段等操作采用串行化的方式
 */
import (
	"github.com/hq-cml/spider-engine/utils/log"
	"fmt"
	"github.com/hq-cml/spider-engine/core/database"
	"errors"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/core/index"
	"github.com/hq-cml/spider-engine/basic"
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

	//删除对应的请求cache和调度scheduler
	for tbName, _ := range db.TableMap {
		dbTable := p.Database + "." + tbName
		tbCache, ok := se.CacheMap[dbTable]
		if ok {
			tbCache.Close()
		}
		delete(se.CacheMap, dbTable)
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

	//启动独立的一对goroutine任务调度，负责处理dml和ddl中的写入任务
	dbTable := p.Database + "." + p.Table
	se.CacheMap[dbTable] = se.doSchedule(dbTable)

	_, err := db.CreateTable(p.Table, fields)
	if err != nil {
		log.Errf("CreateTable Error: %v", err)
		return err
	}

	log.Infof("Create Table: %v", p.Database + "." + p.Table)
	return nil
}

//删表
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

	//删除对应的请求cache和调度scheduler
	dbTable := p.Database + "." + p.Table
	tbCache, ok := se.CacheMap[dbTable]
	if ok {
		tbCache.Close()
	}
	delete(se.CacheMap, dbTable)


	log.Infof("Drop Table: %v", p.Database + "." + p.Table)
	return nil
}

func (se *SpiderEngine) ProcessDDLRequest(req *basic.SpiderRequest) {

	if req.Type == basic.REQ_TYPE_DDL_ADD_FIELD {
		p := req.Req.(*AlterFieldParam)
		db, _ := se.DbMap[p.Database]
		t, _ := index.IDX_MAP[p.Filed.Type]

		//新增
		fld := field.BasicField{
			FieldName: p.Filed.Name,
			IndexType: t,
		}
		err := db.AddField(p.Table, fld)
		if err != nil {
			log.Errf("AddField Error: %v", err)
			req.Resp <- basic.NewResponse(err, nil)
			return
		}

		log.Infof("Add Field: %v", p.Database + "." + p.Table + "." + p.Filed.Name + "." + p.Filed.Type)
		req.Resp <- basic.NewResponse(nil, nil)

	} else if req.Type == basic.REQ_TYPE_DDL_DEL_FIELD {
		p := req.Req.(*AlterFieldParam)
		db, _ := se.DbMap[p.Database]
		err := db.DeleteField(p.Table, p.Filed.Name)
		if err != nil {
			log.Errf("DeleteField Error: %v", err)
			req.Resp <- basic.NewResponse(err, nil)
			return
		}

		log.Infof("Delete Field: %v", p.Database + "." + p.Table + "." + p.Filed.Name)
		req.Resp <- basic.NewResponse(nil, nil)
	} else {
		log.Fatal("Unsupport req.Type:%v", req.Type)
		req.Resp <- basic.NewResponse(errors.New(fmt.Sprintf("Unsupport req.Type:%v", req.Type)), nil)
	}

	return
}

//增字段（串行化）
func (se *SpiderEngine) AddField(p *AlterFieldParam) error {
	if se.Closed {
		return errors.New("Spider Engine is closed!")
	}
	se.RwMutex.RLock()          //读锁
	defer se.RwMutex.RUnlock()

	//校验
	_, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return errors.New("The db not exist!")
	}
	_, ok := index.IDX_MAP[p.Filed.Type]
	if !ok {
		log.Errf("Unsuport index type: %v", p.Filed.Type)
		return errors.New(fmt.Sprintf("Unsuport index type: %v", p.Filed.Type))
	}

	//生成请求放入cache
	req := basic.NewRequest(basic.REQ_TYPE_DDL_ADD_FIELD, p)
	se.CacheMap[p.Database + "." + p.Table].Put(req)
	log.Debug("Put AddField request: ", p.Database + "." +p.Table)

	//等待结果
	resp := <- req.Resp
	if resp.Err != nil {
		return resp.Err
	}

	return nil
}

//减字段（串行化）
func (se *SpiderEngine) DeleteField(p *AlterFieldParam) error {
	if se.Closed {
		return errors.New("Spider Engine is closed!")
	}
	se.RwMutex.RLock()          //读锁
	defer se.RwMutex.RUnlock()

	//校验
	_, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return errors.New("The db not exist!")
	}

	//生成请求放入cache
	req := basic.NewRequest(basic.REQ_TYPE_DDL_DEL_FIELD, p)
	se.CacheMap[p.Database + "." + p.Table].Put(req)
	log.Debug("Put DeleteField request: ", p.Database + "." +p.Table)

	//等待结果
	resp := <- req.Resp
	if resp.Err != nil {
		return resp.Err
	}

	return nil
}













