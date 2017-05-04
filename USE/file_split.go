package main

import (
	dt "dt"
	"time"
	// //"bufio"
	// "bytes"
	"fmt"
	// "math"
	// "os"
	// "strconv"
)

var (
	folder_base = "/Users/edward/work/backup/"
	//folder_Ouputs = "/Users/edward/work/JsonOutputs/"
	folder_Ouputs = "/Users/edward/work/split/"
)

func main() {
	date := "2017-04-30/"
	start := time.Now()
	files := dt.GetFilelist(folder_base + date)
	for _, file := range files {
		dt.File_Split(file, folder_Ouputs+date)
	}
	t2 := time.Now()
	fmt.Printf("Date:%s :output %s took %v\n", date, folder_Ouputs+date, t2.Sub(start))
}
