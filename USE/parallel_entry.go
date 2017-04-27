package main

import (
	dt "dt"
	"fmt"
	"os"
	//"strconv"
	//"io/ioutil"
	"runtime"
	"strings"
	"time"

	"github.com/goinggo/jobpool"
	//"github.com/op/go-logging"
)

type WorkProvider1 struct {
	File string
	Date string
}

func (wp *WorkProvider1) RunJob(jobRoutine int) {
	file := wp.File
	rl := &[]dt.Record{}
	start := time.Now()
	dt.Read_Records_From_File(file, rl)

	absolute_path := strings.Split(file, "/")
	file_name := absolute_path[len(absolute_path)-1]
	date := absolute_path[len(absolute_path)-2]

	if _, err := os.Stat(folder_Ouputs + date + "/"); os.IsNotExist(err) {
		os.Mkdir(folder_Ouputs+date+"/", os.ModePerm)
		fmt.Println("created: " + folder_Ouputs + date + "/")
	}
	dt.Write_json_Array(folder_Ouputs+date+"/"+file_name+".json", rl)

	t2 := time.Now()
	fmt.Printf("Date:%s :rw file %s took %v\n", wp.Date, wp.File, t2.Sub(start))

}

var (
	folder_base = "/Users/edward/work/backup/"
	//folder_Ouputs = "/Users/edward/work/JsonOutputs/"
	folder_Ouputs = "/Users/edward/work/wash/"
)

func ReadFolderBase() {
	jobPool := jobpool.New(runtime.NumCPU(), 1000)

	files1 := dt.GetFilelist("/Users/edward/work/backup/2017-04-26")
	fmt.Printf("%s files %v\n", "2017-04-26", len(files1))
	for _, file := range files1 {
		jobPool.QueueJob("main", &WorkProvider1{file, "2017-04-26"}, false)
	}
	files2 := dt.GetFilelist("/Users/edward/work/backup/2017-04-25")
	fmt.Printf("%s files %v\n", "2017-04-25", len(files2))
	for _, file := range files2 {
		jobPool.QueueJob("main", &WorkProvider1{file, "2017-04-25"}, false)
	}

	files3 := dt.GetFilelist("/Users/edward/work/backup/2017-04-24")
	fmt.Printf("%s files %v\n", "2017-04-24", len(files3))
	for _, file := range files3 {
		jobPool.QueueJob("main", &WorkProvider1{file, "2017-04-24"}, false)
	}

	files4 := dt.GetFilelist("/Users/edward/work/backup/2017-04-23")
	fmt.Printf("%s files %v\n", "2017-04-23", len(files4))
	for _, file := range files4 {
		jobPool.QueueJob("main", &WorkProvider1{file, "2017-04-23"}, false)
	}

	// files5 := dt.GetFilelist("/Users/edward/work/backup/2017-04-12")
	// fmt.Printf("%s files %v\n", "2017-04-16", len(files5))
	// for _, file := range files5 {
	// 	jobPool.QueueJob("main", &WorkProvider1{file, "2017-04-12"}, false)
	// }

	// all_date, _ := ioutil.ReadDir(folder_base)
	// fmt.Println("Reading DIR=", folder_base)
	// for i := len(all_date) - 1; i >= 0; i-- {
	// 	f := all_date[i]
	// 	//for _, f := range all_date {
	// 	//rl := &[]dt.Record{}
	// 	date := f.Name()
	// 	files := dt.GetFilelist(folder_base + date)
	// 	fmt.Printf("%s files %v\n", date, len(files))

	// 	for _, file := range files {
	// 		jobPool.QueueJob("main", &WorkProvider1{file, date}, false)
	// 	}
	// }

}

func ReadFolder(folder string) {
	jobPool := jobpool.New(runtime.NumCPU(), 1000)

	fmt.Printf("*******> QW: %d AR: %d\n",
		jobPool.QueuedJobs(),
		jobPool.ActiveRoutines())

	//rl := &[]dt.Record{}
	absolute_path := strings.Split(folder, "/")
	date := absolute_path[len(absolute_path)-1]

	files := dt.GetFilelist(folder)
	fmt.Printf("%s files %v\n", date, len(files))

	for _, file := range files {
		jobPool.QueueJob("main", &WorkProvider1{file, date}, false)
	}
}

func main() {

	// if len(os.Args) == 2 {
	// 	folder_base = os.Args[1]
	// 	folder_Ouputs = os.Args[2]
	// }

	runtime.GOMAXPROCS(runtime.NumCPU())

	ReadFolderBase()
	//ReadFolder("/Users/edward/work/backup/2017-04-17")

	var input string
	fmt.Scanln(&input)
	fmt.Println("done")
}
