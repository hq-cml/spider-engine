![标题](./img/spider-engine-title.png)

# 基于Golang的迷你版搜索引擎

Spider-Engine是一款基于Go实现的的迷你搜索引擎，参考借鉴了Mysql、通用SE等的实现方案。设计目标简单、直接、性能高效、稳定皮实~

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
	对于各类存储，很多概念都类似、通用，但又有些许区别，比如同一个表的概念，在Mysql中称之为表，在ES中称之为index。这里统一解释拉平对齐，便于消除歧义。

概念名称 | Mysql | ES | Spider-Engine
---|---|---|---|---
- | Database | Indexes| Database
- | Table| Index/Type| Table
- |Row| Document| Document
- | Column| Field| Field

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

##### 建库：

```
{
    "database":"sp_db"
}

```


##### 建表：

```
{
	"database":"sp_db",
	"table":"user",
	"fields":[
		{"name":"user_id", "type":"primary"},
		{"name":"user_name", "type":"whole"},
		{"name":"agent", "type":"number"},
		{"name":"user_desc", "type":"words"}
	]
}
```

##### 增、减字段：

```
{
	"database":"sp_db",
	"table":"user",
	"field": {"name":"user_desc", "type":"words"}
}
```


##### 增加文档：

```
{
	"database":"sp_db",
	"table":"user",
	"content":{
		"user_id":"10001",
		"user_name":"张三",
		"date":23,
		"user_desc":"喜欢看书，也喜欢运动。他是一个文武兼备的人。"
	}
}
```

##### 删除文档：

```
{
	"database":"test",
	"table":"user",
	"primary_key":"10004"
}
```


##### 编辑文档：

```
{
	"database":"sp_db",
	"table":"user",
	"content":{
		"user_id":"10001",
		"user_name":"张三",
		"date":23,
		"user_desc":"喜欢看书，也喜欢运动。他是一个文武兼备的人。"
	}
}
```


##### 获取文档：

```
_getDoc?db=sp_db&table=weibo&primary_key=10001
```


##### 搜索：

```
{
	"database":"sp_db",
	"table":"user",
	"field_name":"user_desc",
	"value":"运动"
}
```

##### 跨字段搜索：

```
{
	"database":"sp_db",
	"table":"user",
	"value":"运动"
}
```


##### 过滤器：



#### TODO：
- 1. 分布式高可用存储，目前思路是引入etcd，或者将etcd-raft模块移植过来使用
- 2. 分布式得搜索，区别于分布式的存储
- 3. 更加智能的排序规则，目前仅支持简单的DF-IDF算法
- 4. 更高的并发性能，支持并发保证一致性，内部采用了一些读写锁和channel做串行化，对性能有一定的损伤
- 5. 搜索性能加速，后续可以实施多个分区同时并发进行搜索

