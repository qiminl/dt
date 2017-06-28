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

var (
	folder_backup_base = "/Users/edward/work/backup/2017/"

	folder_base = "/Users/edward/work/split/"
	//folder_Ouputs = "/Users/edward/work/JsonOutputs/"
	folder_Ouputs = "/Users/edward/work/wash/"

	date_arg = "06-19"
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

func ReadFolderBase() {
	jobPool := jobpool.New(runtime.NumCPU(), 1000)
	files1 := dt.GetFilelist("/Users/edward/work/split/" + date_arg)
	fmt.Printf("%s files %v\n", date_arg, len(files1))
	for _, file := range files1 {
		jobPool.QueueJob("main", &WorkProvider1{file, date_arg}, false)
	}

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

//Original asb files might be too large to handle at onece, so split it;
func FileSplit() {
	date := date_arg + "/"
	start := time.Now()
	files := dt.GetFilelist(folder_backup_base + date)
	for _, file := range files {
		dt.File_Split(file, folder_base+date)
	}
	t2 := time.Now()
	fmt.Printf("Date:%s :output %s took %v\n", date, folder_base+date, t2.Sub(start))
}

func RemoveSplit() {
	os.RemoveAll(folder_base + date_arg + "/")
}

func main() {

	//get date for use;
	if len(os.Args) >= 1 {
		date_arg = os.Args[1]
		//folder_Ouputs = os.Args[2]
	} else {
		panic(fmt.Printf("Need date as argument in mm-dd format"))
		return 0
	}

	runtime.GOMAXPROCS(runtime.NumCPU())
	start := time.Now()
	//split original data file into smaller piece.
	FileSplit()

	//parallel output
	ReadFolderBase()
	//ReadFolder("/Users/edward/work/backup/2017-04-17")

	//removing intermediate files
	RemoveSplit()

	t2 := time.Now()
	fmt.Printf("Date:%s done split&parallel&delete, took %v\n", date, t2.Sub(start))

	var input string
	fmt.Scanln(&input)
	fmt.Println("done")
}
