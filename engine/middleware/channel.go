package middleware

type CommonChannel struct {
	flag string            		//通道标志
	cap  int                    //通道容量
	ch   chan interface{}       //通道载体

}

func NewCommonChannel(capacity int, flag string) *CommonChannel {
	return &CommonChannel{
		cap:  capacity,
		ch:   make(chan interface{}, capacity),
		flag: flag,
	}
}

//实现SpiderChannel接口
func (c *CommonChannel) Put(data interface{}) error {
	c.ch <- data
	return nil
}
func (c *CommonChannel) Get() (interface{}, bool) {
	data, ok := <-c.ch
	return data, ok
}
func (c *CommonChannel) Len() int {
	return len(c.ch)
}
func (c *CommonChannel) Cap() int {
	return c.cap
}
func (c *CommonChannel) Close() {
	close(c.ch)
}