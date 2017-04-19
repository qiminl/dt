package main

import (
	dt "dt"
	"fmt"
	"os"
	//"strconv"
	"encoding/json"
	"io/ioutil"
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
	start := time.Now()

	rl := &dt.RecordList{}
	configFile, err := os.Open(file)
	if err != nil {
		fmt.Println("opening json file", err.Error())
	}
	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&rl); err != nil {
		fmt.Println("parsing config file", err.Error())
	}

	TrafficList := make(map[string][]dt.Record)
	for index := range (*rl).Records {
		TrafficList[(*rl).Records[index].Campaign.App_id] = append(TrafficList[(*rl).Records[index].Campaign.App_id], (*rl).Records[index])
	}

	// absolute_path := strings.Split(file, "/")
	// ExportJson(absolute_path, rl)

	t2 := time.Now()
	fmt.Printf("Date:%s :rw file %s took %v\n", wp.Date, wp.File, t2.Sub(start))

}

var (
	folder_Ouputs = "/Users/edward/work/WASH/"
	folder_base   = "/Users/edward/work/JsonOutputs/"
)

// func each_file(files []string) {
// 	fmt.Printf("hmmm?")
// 	for _, file := range files {
// 		println("file=", file)
// 		rl := &[]dt.Record{}
// 		file_rw(file, rl)
// 		//fmt.Println("rl: ", len(*rl))
// 	}
// }

func ExportJson(absolute_path []string, rl *dt.RecordList) {
	file_name := absolute_path[len(absolute_path)-1]
	date := absolute_path[len(absolute_path)-2]
	if _, err := os.Stat(folder_Ouputs + date + "/"); os.IsNotExist(err) {
		os.Mkdir(folder_Ouputs+date+"/", os.ModePerm)
		fmt.Println("created: " + folder_Ouputs + date + "/")
	}

	dt.Write_json_Array_RL(folder_Ouputs+date+"/"+file_name, rl)
}

func ReadFolderBase() {
	jobPool := jobpool.New(runtime.NumCPU(), 1000)

	all_date, _ := ioutil.ReadDir(folder_base)
	fmt.Println("Reading DIR=", folder_base)
	for _, f := range all_date {
		//rl := &[]dt.Record{}
		date := f.Name()

		files := dt.GetFilelist(folder_base + date)
		fmt.Printf("%s files %v\n", date, len(files))

		for _, file := range files {
			jobPool.QueueJob("main", &WorkProvider1{file, date}, false)
		}
	}

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

	//ReadFolderBase()

	file := "/Users/edward/work/JsonOutputs/2017-04-13/BB9B098D7A2CE06_00000.asb.json"
	rl := &dt.RecordList{}
	configFile, err := os.Open(file)
	if err != nil {
		fmt.Println("opening json file", err.Error())
	}
	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&rl); err != nil {
		fmt.Println("parsing config file", err.Error())
	}

	TrafficList := make(map[string][]dt.Record)
	for index := range (*rl).Records {
		key := "app_id:" + (*rl).Records[index].Campaign.App_id + ",size:" + (*rl).Records[index].User.Size
		TrafficList[key] = append(TrafficList[key], (*rl).Records[index])
	}

	for index := range TrafficList {
		fmt.Println("key: ", index)
	}

	for index := range (*rl).Records {
		key := "app_id:" + (*rl).Records[index].Campaign.App_id + ",size:" + (*rl).Records[index].User.Size
		TrafficList[key] = append(TrafficList[key], (*rl).Records[index])
	}

	//ReadFolder("/Users/edward/work/backup/2017-04-17")

	var input string
	fmt.Scanln(&input)
	fmt.Println("done")
}
