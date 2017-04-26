package main

import (
	dt "dt"
	//"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/goinggo/jobpool"
	//"github.com/op/go-logging"
)

var (
	folder_Ouputs       = "/Users/edward/work/WASH/"
	folder_base         = "/Users/edward/work/JsonOutputsTest"
	folder_Ouputs_no_os = "/Users/edward/work/WASH_no_os_test/"
	//os_flag             = false
)

type WorkProvider1 struct {
	File string
	Date string
}

type FolderReader struct {
	Folder string
	Date   string
}

type TrafficRatio struct {
	Counter int `json:"counter"`

	Conn_type    int `json:"conn_type"`
	Carrier_code int `json:"carrier_code"`
	Operator     int `json:"operator"`

	Device_mac  int `json:"device_mac"`
	Device_type int `json:"device_type"`
	Device_ifa  int `json:"device_ifa"`
	Ios_ifa     int `json:"ios_ifa"`
	Android_id  int `json:"android_id"`

	Status  int `json:"status"`
	Smaato  int `json:"smaato"`
	VoiceAd int `json:"voicead"`
}

func (wp *FolderReader) RunJob(jobRoutine int) {

	start := time.Now()
	fmt.Printf("start:", wp.Folder)

	TrafficList := make(map[string]*TrafficRatio) // int) //[]dt.Record)
	//ReadFolderBase()
	files := dt.GetFilelist(wp.Folder)
	fmt.Printf("%s files %v\n", wp.Date, len(files))

	for _, file := range files {

		rl := &dt.RecordList{}
		configFile, err := os.Open(file)
		if err != nil {
			fmt.Println("opening json file", err.Error())
		}
		jsonParser := json.NewDecoder(configFile)
		if err = jsonParser.Decode(&rl); err != nil {
			fmt.Println("parsing config file", err.Error())
		}

		for index := range (*rl).Records {

			//V Adn, Publisher,  Ad Unit Size requested ad unit size,
			// App Ad Unit ID, requested OS
			//Ad Unit Size in Platform,  App Name, App OS,
			key := (*rl).Records[index].Campaign.Pub_v_id + "," +
				(*rl).Records[index].Campaign.App_id + "," +
				(*rl).Records[index].User.Size
			//TrafficList[key] += 1 // = append(TrafficList[key], (*rl).Records[index]) //
			//value := TrafficList[key]
			TrafficList[key].Counter++
			if (*rl).Records[index].Device.Conn_type != "Unknown" {
				TrafficList[key].Conn_type++
			}
			if (*rl).Records[index].Device.Carrier_code != "Unknown" {
				TrafficList[key].Carrier_code++
			}
			if (*rl).Records[index].Device.Operator != "Unknown" {
				TrafficList[key].Operator++
			}
			if (*rl).Records[index].Device.Device_mac != "" {
				TrafficList[key].Device_mac++
			}
			if (*rl).Records[index].Device.Device_type != "Unknown" {
				TrafficList[key].Device_type++
			}
			if (*rl).Records[index].Device.Device_ifa != "" {
				TrafficList[key].Device_ifa++
			}
			if (*rl).Records[index].Device.Ios_ifa != "" {
				TrafficList[key].Ios_ifa++
			}
			if (*rl).Records[index].Device.Android_id != "" {
				TrafficList[key].Android_id++
			}
			if (*rl).Records[index].Campaign.Status != "noad" {
				TrafficList[key].Status++
			}
			if (*rl).Records[index].Campaign.Bidder == "smaato" {
				TrafficList[key].Smaato++
			}
			if (*rl).Records[index].Campaign.Bidder == "voicead" {
				TrafficList[key].VoiceAd++
			}
			//TrafficList[key]
		}
		fmt.Println("file: %s done", wp.Folder)
	}
	/*
		if os_flag {
			path := folder_Ouputs + wp.Date
			if _, err := os.Stat(path); os.IsNotExist(err) {
				os.Create(path)
			}
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				panic(err)
			}

			defer f.Close()

			//fmt.Println("pub_v_id, app_id, size")
			for traffic := range TrafficList {
				heads := strings.Split(traffic, ",")
				//fmt.Println(heads[0], ", ",heads[1], ", ", heads[2], " = ", len(TrafficList[traffic]))
				//word := heads[0]+ ", "+heads[1]+ ", "+ heads[2]+ " = "+ len(TrafficList[traffic])
				if _, err = f.WriteString("\n" + heads[0] + ", " + heads[1] + ", " + heads[2] + " = " + strconv.Itoa(len(TrafficList[traffic]))); err != nil {
					panic(err)
				}
				os_map := make(map[string]int)
				//fmt.Println(TrafficList[traffic][0].Device.Os_v)
				for index := range TrafficList[traffic] {
					key := TrafficList[traffic][index].Device.Os_n + " + " + TrafficList[traffic][index].Device.Os_v
					os_map[key] += 1
				}
				for index2 := range os_map {
					//fmt.Println("\tos: ",index2, " = ", os_map[index2])
					//word :="\tos: "+index2+ " = "+ os_map[index2]
					if _, err = f.WriteString("\tos: " + index2 + " = " + strconv.Itoa(os_map[index2]) + "\n"); err != nil {
						panic(err)
					}
				}

			}
		} else {
	*/
	path := folder_Ouputs_no_os + wp.Date + ".csv"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Create(path)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// if _, err = f.WriteString("pub_v_id, app_id, size, number, date"); err != nil {
	// 	panic(err)
	// }
	for traffic := range TrafficList {
		heads := strings.Split(traffic, ",")
		//fmt.Println(heads[0], ", ",heads[1], ", ", heads[2], " = ", len(TrafficList[traffic]))
		//word := heads[0]+ ", "+heads[1]+ ", "+ heads[2]+ " = "+ len(TrafficList[traffic])
		//if _, err = f.WriteString("\n" + heads[0] + ", " + heads[1] + ", " + heads[2] + ", " + strconv.Itoa(TrafficList[traffic]) + ", " + wp.Date); err != nil {
		if _, err = f.WriteString("\n" + heads[0] + ", " + heads[1] + ", " + heads[2] + ", " +
			strconv.Itoa(TrafficList[traffic].Android_id) + ", " + strconv.Itoa(TrafficList[traffic].Carrier_code) + ", " +
			strconv.Itoa(TrafficList[traffic].Conn_type) + ", " + strconv.Itoa(TrafficList[traffic].Smaato) + ", " +
			strconv.Itoa(TrafficList[traffic].VoiceAd)); err != nil {
			panic(err)
		}
	}
	//}
	//ioutil.WriteFile(path, RecordList2B, 0644)
	t2 := time.Now()
	fmt.Printf("Date:%s :rw file %s took %v\n", wp.Date, wp.Folder, t2.Sub(start))

}

func ReadFolderWash(folder string) {
	jobPool := jobpool.New(runtime.NumCPU(), 1000)

	all_date, _ := ioutil.ReadDir(folder)
	fmt.Println("Reading DIR=", folder)
	for i := len(all_date) - 1; i >= 0; i-- {
		f := all_date[i]
		//for _, f := range all_date {
		//rl := &[]dt.Record{}
		date := f.Name()
		//fmt.Println("folder name = ", folder+"/"+date)
		jobPool.QueueJob("main", &FolderReader{folder + "/" + date, date}, false)
	}
}

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

func main() {

	// if len(os.Args) == 2 {
	// 	folder_base = os.Args[1]
	// 	folder_Ouputs = os.Args[2]
	// }

	runtime.GOMAXPROCS(runtime.NumCPU())

	// jobPool := jobpool.New(runtime.NumCPU(), 1000)

	// jobPool.QueueJob("main", &FolderReader{folder_base + "/" + "2017-04-16", "2017-04-16"}, false)
	// jobPool.QueueJob("main", &FolderReader{folder_base + "/" + "2017-04-12", "2017-04-12"}, false)
	// jobPool.QueueJob("main", &FolderReader{folder_base + "/" + "2017-04-16", "2017-04-16"}, false)
	// jobPool.QueueJob("main", &FolderReader{folder_base + "/" + "2017-04-13", "2017-04-13"}, false)

	//wash os//no_os
	ReadFolderWash(folder_base)

	//ReadFolder("/Users/edward/work/backup/2017-04-17")

	var input string
	fmt.Scanln(&input)
	fmt.Println("done")
}
