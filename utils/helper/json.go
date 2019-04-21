package helper

import "encoding/json"

func JsonEncode(d interface{}) string {
	s, e := json.Marshal(d)
	if e != nil {
		return e.Error()
	} else {
		return string(s)
	}
}

func JsonEncodeIndent(d interface{}) string {
	s, e := json.MarshalIndent(d, "", "  ")
	if e != nil {
		return e.Error()
	} else {
		return string(s)
	}
}