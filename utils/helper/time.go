package helper

import "time"

//字符串转时间戳
func String2Timestamp(datetime string) (int64, error) {
	var timestamp time.Time
	var err error

	if len(datetime) > 10 {
		timestamp, err = time.ParseInLocation("2006-01-02 15:04:05", datetime, time.Local)
		if err != nil {
			return 0, err
		}
	} else {
		timestamp, err = time.ParseInLocation("2006-01-02", datetime, time.Local)
		if err != nil {
			return 0, err
		}
	}
	return timestamp.Unix(), nil
}

//时间戳转字符串
func Timestamp2String(timestamp int64) string {
	tm := time.Unix(timestamp, 0)
	return tm.Format("2006-01-02 15:04:05")
}
