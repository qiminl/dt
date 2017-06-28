package main

import (
	dt "dt"
	"fmt"
	"time"
)

var (
	folder_backup_base = "/Users/edward/work/backup/2017/"
	//folder_Ouputs = "/Users/edward/work/JsonOutputs/"
	folder_Ouputs = "/Users/edward/work/split/"
)

func main() {
	date := "06-19/"
	start := time.Now()
	files := dt.GetFilelist(folder_backup_base + date)
	for _, file := range files {
		dt.File_Split(file, folder_Ouputs+date)
	}
	t2 := time.Now()
	fmt.Printf("Date:%s :output %s took %v\n", date, folder_Ouputs+date, t2.Sub(start))
}
