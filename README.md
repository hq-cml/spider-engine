![标题](./img/spider-engine-title.png)

# 基于Golang的小型搜索引擎

Spider-Engine是一款基于Go实现的小型搜索引擎，简单、直接、高效、皮实。设计上参考借鉴了Mysql、ES等。

**Tips:**  
    目前Spider-Engine已经和 [Spider-Man](https://github.com/hq-cml/spider-man)（一个Go的爬虫框架项目）打通。  
    并且由 [Spider-Face](https://github.com/hq-cml/spider-man)（一个Go的web框架项目）配了一套简单的搜索引擎页面。  
    当然，这并非必须的，您也可以自己实现爬虫将数据结构化之后导入并选择自己熟悉的web框架。

#### 支持的功能：
- 1. 类比Mysql和ES，支持库、表、字段级别的存储单元管理
- 2. 字段支持字符串类型、数字类型、时间类型
- 3. 字符类型的字段，可以选择分词模式和完整模式
- 4. 底层实现倒排、正排索引，支持搜索和字段信息获取
- 5. 文本分词（基于jiebago）
- 6. 支持搜索 & 过滤 & 排序
- 7. 搜索结果排序，基于TF-IDF算法
- 8. 单个字段全文索引，SE区别于Mysql的最大特点
- 9. 跨多字段全文索引（所有字符串类型的字段）
- 10. 底层分区，类似于Mysql的分区概念
- 11. 底层分区自动创建、合并，上层无感
- 12. 读写并发安全，同时进行文档增删改、索引建立、查询
- 13. Restful风格接口
- 14. 尽量做到无外部依赖，小部分已经用govendor做了自依赖

#### 不支持的功能：
- 1. 数字类型的倒排索引
- 2. 浮点类型，请先用字符串类型代替
- 3. 分布式

#### 概念解释与对齐：
	对于各类存储，很多概念都类似、通用，但又有些许区别，比如同一个表的概念，在Mysql中称之为Table，在ES中称之为index或type。
	这里统一解释拉平对齐，便于消除歧义。

Mysql | ES | Spider-Engine
---|---|---
Database | Indexes| Database
Table| Index/Type| Table
Row| Document| Document
Column| Field| Field


这里我参考并结合了Mysql和ES的命名规则，一个原则是让头一次看的人一眼就看明白，没什么歧义。具体的解释，见：[开发文档](./design.md)
#### 获取与编译：

```
go get github.com/hq-cml/spider-engine
cd $GOPATH/src/github.com/hq-cml/spider-engine/
go build ./
```


#### 安装与启动：

```
export SPIDER_PATH=/tmp/spider-engine   #期望的部署路径
./install.sh
cd $SPIDER_PATH
./spider-engine
```



#### 关于支持的字段类型：
    目前spider-engine一共支持6种字段type，详细如下：

类型 | 说明
---|---
primary | 主键，目前仅支持字符型，如果用户不创建主键，那么spider会自动创建默认主键
whole | 单一字符型，系统生成倒排的时候，不会进行分词，即全词匹配检索，使用场景诸如：姓名，唯一Id等等
number | 数字型，目前只支持整数，系统底层不会为number类型建立倒排
time | 时间类型，以字符类型传入，目前支持'2019-05-11' 或者'2019-05-11 08:30:00'两种形式，底层支持对时间类型进行过滤和排序
words | 普通字符型，该类型字段会进行分词器分词，是搜索引擎与Mysql的最大区别所在，分词后的字段可以进行快速检索，适用于人员介绍、评价等等。
pure | 纯字符类型，该类型不会建立倒排索引，仅拥有正排索引，不支持对该字段进行检索，用于完整文档的获取与现实。


#### 接口使用说明：
    spider-engine接口整体采用RestFul风格：

##### 整体状态详情：
```
curl -X GET 'http://127.0.0.1:9528/_status'
```

##### 建库：
```
curl -X POST 'http://127.0.0.1:9528/sp_db'
```

##### 删库：
```
curl -X DELETE 'http://127.0.0.1:9528/sp_db'
```

##### 建表：
```
curl -X POST 'http://127.0.0.1:9528/sp_db/user' -d '[
	{"name":"user_id", "type":"primary"},
	{"name":"user_name", "type":"whole"},
	{"name":"age", "type":"number"},
	{"name":"user_desc", "type":"words"}
]'
```

##### 删除表：
```
curl -X DELETE 'http://127.0.0.1:9528/sp_db/user'
```

##### 增字段：
```
curl -X PATCH 'http://127.0.0.1:9528/sp_db/user' -d '{
	"type":"addField",
	"field": {"name":"tobe_del", "type":"words"}
}'
```

##### 删字段：
```
curl -X PATCH 'http://127.0.0.1:9528/sp_db/user' -d '{
	"type":"delField",
	"field": {"name":"tobe_del", "type":"words"}
}'
```

##### 增加文档：
```
curl -X POST 'http://127.0.0.1:9528/sp_db/user/10001' -d '{
    "user_id" : "10001",
	"user_name":"张三",
	"age":23,
	"user_desc":"喜欢文学，也喜欢运动，是个好青年"
}'
```
    btw: 主键user_id在url path中必填, 在http body中可以填也可以不填,如果填需要和path中保持一致.
         如果没有主键,可以用_auto代替, spider会在底层自动生成主键
```
curl -X POST 'http://127.0.0.1:9528/sp_db/user/_auto' -d '{
	"user_name":"张三",
	"age":23,
	"user_desc":"喜欢文学，也喜欢运动，是个好青年"
}'
```

##### 删除文档：
```
curl -X DELETE 'http://127.0.0.1:9528/sp_db/user/10001'
```

##### 编辑文档：
```
curl -X PUT 'http://127.0.0.1:9528/sp_db/user/10001' -d '{
    "user_id" : "10001",
	"user_name":"唐伯虎",
	"age":23,
	"user_desc":"喜欢秋香"
}'
```
    btw: 主键user_id在url path中必填, 在http body中可以填也可以不填,如果填需要和path中保持一致.

```
curl -X PUT 'http://127.0.0.1:9528/sp_db/user/10001' -d '{
	"user_name":"祝枝山",
	"age":23,
	"user_desc":"喜欢石榴姐"
}'
```
##### 获取文档：
```
curl -X GET 'http://127.0.0.1:9528/sp_db/user/10001'
```

##### 搜索：
```
curl -X GET 'http://127.0.0.1:9528/_search' -d '{
	"database":"sp_db",
	"table":"user",
	"fieldName":"user_desc",
	"value":"秋香"
}'
```

##### 跨字段搜索：
如果不指定具体查询字段，那么spider会自动对支持倒排的所有字段进行搜索，实现跨字段搜索之功能。
```
curl -X GET 'http://127.0.0.1:9528/_search' -d '{
	"database":"sp_db",
	"table":"user",
	"value":"秋香"
}'
```

##### 分页：
分页参数为offset和size，如下:
```
curl -X GET 'http://127.0.0.1:9528/_search' -d '{
	"database": "sp_db",
	"table": "user",
	"value": "秋香",
	"offset": 10,
	"size": 10
}'
```

##### 过滤器：
Spider引擎支持简单的过滤器，过滤器用于搜索结果的进一步缩小。
比如有一个需求，希望找到喜欢秋香的人，并且希望这些人年龄在20到30之间。则可以

```
curl -X GET 'http://127.0.0.1:9528/_search' -d '{
	"database":"sp_db",
	"table":"user",
	"value":"秋香",
	"filters":[
	    {
	        "field": "age", "type": "between", "begin": 20, "end": 30
	    }
	]
}'
```

目前支持的所有的过滤器：
```
type SearchFilter struct {
	FieldName       string   `json:"field"`   //需要过滤的字段，和搜索字段不是一个东西
	FilterType      string   `json:"type"` 	  //过滤类型: =, !=, >, <, in, not in, between, prefix, suffix, contain
	StrVal          string   `json:"str"` 	  //用于字符的==/!=, prefix, suffix
	IntVal          int64    `json:"int"` 	  //用于数字的==/!=/>/<
	Begin           int64    `json:"begin"`   //用于数字between
	End             int64    `json:"end"` 	  //用于数字between
	RangeNums       []int64  `json:"iranges"` //用于数字in或not in
	RangeStrs       []string `json:"sranges"` //用于字符in或not in
}
```
其中
- prefix, suffix, contain仅支持字符串
- < , >, between仅支持数字

##### 返回结果

```
{
	"code": 0,
	"msg": "ok",
	"data": {
		"docs": [{
			"Key": "10001",
			"Detail": {
				"user_id": "10001",
				"user_name": "唐伯虎",
				"age": 23,
				"user_desc": "喜欢秋香"
			}
		}],
		"total": 1
	}
}
```
说明：
- code和msg表示请求的结果状态
- data表示具体返回结果
- data.total表示一共搜索条数（总条数）
- data.docs表示文档列表

#### 开发文档：
[文档](./design.md)

#### TODO：
- 1. 分布式高可用存储，目前思路是引入etcd，或者将etcd-raft模块移植过来使用
- 2. 更加智能的排序规则，目前仅支持简单的DF-IDF算法
- 3. 更高的并发性能，支持并发保证一致性，内部采用了一些读写锁和channel做串行化，对性能有一定的损伤
- 4. 搜索性能加速，后续可以实施多个分区同时并发进行搜索
