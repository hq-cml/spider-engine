package helper

import (
	"encoding/json"
)

func JsonEncode(d interface{}) string {
	if d == nil {
		return ""
	}
	s, e := json.Marshal(d)
	if e != nil {
		return ""
	} else {
		return string(s)
	}
}

func JsonEncodeIndent(d interface{}) string {
	if d == nil {
		return ""
	}
	s, e := json.MarshalIndent(d, "", "  ")
	if e != nil {
		return ""
	} else {
		return string(s)
	}
}