package middleware
/*
 * 请求缓冲
 * 用一个带锁保护的slice实现缓冲的效果
 * golang源码中, 很多的buf都是用slice来实现的缓冲效果
 */
import (
	"fmt"
	"github.com/hq-cml/spider-man/basic"
	"sync"
)

// 请求缓存的实现类型。
type RequestCache struct {
	cache  []*basic.Request // 请求的缓存介质。
	mutex  sync.Mutex       // 互斥锁。
	status int              // 缓存状态。0表示正在运行，1表示已关闭。
}

const (
	REQUEST_CACHE_STATUS_RUNNING = 0
	REQUEST_CACHE_STATUS_COLOSED = 1
)

//状态字典。
var statusMap = map[int]string {
	REQUEST_CACHE_STATUS_RUNNING: "running",
	REQUEST_CACHE_STATUS_COLOSED: "closed",
}

// 创建请求缓存。
func NewRequestCache() *RequestCache {
	rc := &RequestCache {
		cache: make([]*basic.Request, 0),
	}
	return rc
}

// 将请求放入请求缓存。
func (rc *RequestCache) Put(req *basic.Request) bool {
	if req == nil {
		return false
	}
	if rc.status == REQUEST_CACHE_STATUS_COLOSED {
		return false
	}
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	rc.cache = append(rc.cache, req)
	return true
}

//从请求缓存获取最早被放入且仍在其中的请求。
//如果cache是空, 则返回nil
func (rc *RequestCache) Get() *basic.Request {
	if rc.Length() == 0 {
		return nil
	}
	if rc.status == REQUEST_CACHE_STATUS_COLOSED {
		return nil
	}
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	req := rc.cache[0] //从头取第一个
	rc.cache = rc.cache[1:]
	return req
}

// 获得请求缓存的容量。
func (rc *RequestCache) Capacity() int {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	return cap(rc.cache)
}

// 获得请求缓存的实时长度，即：其中的请求的即时数量。
func (rc *RequestCache) Length() int {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	return len(rc.cache)
}

// 关闭请求缓存。
func (rc *RequestCache) Close() {
	if rc.status == REQUEST_CACHE_STATUS_COLOSED {
		return
	}
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	rc.status = REQUEST_CACHE_STATUS_COLOSED
}

// 摘要信息
func (rc *RequestCache) Summary(prefix string) string {
	summary := fmt.Sprintf(prefix + "Status: %s\n" + prefix + "Len: %d, " + "Cap: %d\n",
		statusMap[rc.status],
		rc.Length(),
		rc.Capacity())
	return summary
}
