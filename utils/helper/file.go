package helper

import (
	"os"
	"io/ioutil"
	"github.com/hq-cml/spider-engine/utils/log"
)

//读取file文件
func ReadFile(filePath string) ([]byte, error) {
	fin, err := os.Open(filePath)
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

// 判断所给路径是否为文件夹
func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

// 判断所给路径是否为文件夹
func Mkdir(path string) bool {
	err := os.MkdirAll(path, 755)
	if err != nil {
		return false
	}
	return true
}

// 写文件
// Note：
//   覆盖写，不是追加写
func OverWriteToFile(data []byte, filePath string) error {
	fout, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer fout.Close()

	_, err = fout.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func Remove(path string) error {
	if err := os.Remove(path); err != nil {
		log.Err("Remove Error: ", err)
		return err
	}
	log.Info("Remove: ", path)
	return nil
}