package main

import (
	//dt "dt"
	//"bufio"
	"fmt"
	"math"
	"os"
	//"strconv"
)

var (
	folder_base = "/Users/edward/work/ttt/2017-04-17/"
	//folder_Ouputs = "/Users/edward/work/JsonOutputs/"
	folder_Ouputs = "/Users/edward/work/wash/"
)

const chunkSize int64 = 4 << 6

func main() {
	//file_path := folder_base + "BB9B098D7A2CE06_00000.asb"
	file_test := folder_base + "test.asb"

	fileInfo, err := os.Stat(file_test)
	if err != nil {
		fmt.Println(err)
	}
	num := int64(math.Ceil(float64(fileInfo.Size()) / float64(chunkSize)))
	fmt.Println("Total Chunks# ", num, " size:", float64(fileInfo.Size()))
}
