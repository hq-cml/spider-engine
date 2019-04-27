package helper

import (
	"github.com/hq-cml/spider-engine/utils/random"
	"bytes"
	"fmt"
	"time"
	"strconv"
)

func GenUuid() string {
	var buff bytes.Buffer
	buff.WriteString(random.GenRandString(5))
	buff.WriteString("-")
	buff.WriteString(fmt.Sprintf("%05v", (time.Now().UnixNano()/3) % 100000))
	buff.WriteString("-")
	buff.WriteString(random.GenRandString(5))
	buff.WriteString("-")
	buff.WriteString(strconv.Itoa(int(random.GenRandIntMinMax(10000, 99999))))

	return buff.String()
}