package helper

import (
	"os"
	"io/ioutil"
)

//读取file文件
func ReadFile(file_name string) ([]byte, error) {
	fin, err := os.Open(file_name)
	defer fin.Close()
	if err != nil {
		return nil, err
	}

	buffer, err := ioutil.ReadAll(fin)
	if err != nil {
		return nil, err
	}
	return buffer, nil

}

// 判断所给路径文件/文件夹是否存在
func Exist(path string) bool {
	_, err := os.Stat(path)    //os.Stat获取文件信息
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

