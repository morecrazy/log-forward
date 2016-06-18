package main

import(
	"io/ioutil"
)

func GetFolderFileNames() []string {
	fileNameList := []string{}
	// 获取所有文件
	files, _ := ioutil.ReadDir(gFolderPath)
	for _,file := range files {
		if file.IsDir() {
			continue
		} else {
			fileNameList = append(fileNameList, file.Name())
		}
	}
	return fileNameList
}
