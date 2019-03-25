package json

import "encoding/json"

func JsonEnocde(d interface{}) string {
	s, e := json.Marshal(d)
	if e != nil {
		return e.Error()
	} else {
		return string(s)
	}
}