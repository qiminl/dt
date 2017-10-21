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
	folder_Ouputs       = "/Users/edward/work/wash_output/"
	folder_base         = "/Users/edward/work/wash"
	folder_Ouputs_no_os = "/Users/edward/work/WASH_no_os_test/"
	//os_flag             = false

	jobPool = jobpool.New(runtime.NumCPU(), 1000)
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
	Lat          int `json:"lat"`
	Lon          int `json:"lon"`

	Device_mac  int `json:"device_mac"`
	Device_type int `json:"device_type"`
	Device_ifa  int `json:"device_ifa"`
	Ios_ifa     int `json:"ios_ifa"`
	Android_id  int `json:"android_id"`
	Bundle      int `json:"bundle"`

	Status    int `json:"status"`
	Smaato    int `json:"smaato"`
	VoiceAd   int `json:"voicead"`
	CoolPad   int `json:"coolpad"`
	HelloGame int `json:"hellogame"`
}

//parallization of each Date (folder in this case)
func (wp *FolderReader) RunJob(jobRoutine int) {
	fmt.Println("enter once")

	start := time.Now()
	fmt.Printf("start:", wp.Folder)

	/**
	to create a report on % of fields, %SSPs, etc
	*/
	// path := folder_Ouputs_no_os + wp.Date + ".csv"
	// trafficListReport(wp, path)

	vadn := "smaato"
	ImpsReport(vadn, wp.Folder, wp.Date)
	//AdnReport(vadn, wp.Folder, wp.Date)

	//ioutil.WriteFile(path, RecordList2B, 0644)
	t2 := time.Now()
	fmt.Printf("Date:%s :output took %v\n", wp.Date, t2.Sub(start))
	fmt.Printf("*******> QW: %d AR: %d CPU:%d\n",
		jobPool.QueuedJobs(),
		jobPool.ActiveRoutines(),
		runtime.NumCPU())

}

func ReadFolderWash(folder string) {

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

func main() {

	date := "08-04"
	///get date for use;
	if len(os.Args) >= 1 {
		date = os.Args[1]
		//folder_Ouputs = os.Args[2]
	} else {
		// panic(fmt.Println("Need date as argument in mm-dd format"))
		return
	}

	runtime.GOMAXPROCS(runtime.NumCPU())
	//ReadFolderWash(folder_base)

	jobPool := jobpool.New(runtime.NumCPU(), 1000)
	jobPool.QueueJob("main", &FolderReader{folder_base + "/" + date, date}, false)
	// jobPool.QueueJob("main", &FolderReader{folder_base + "/" + "2017-04-12", "2017-04-12"}, false)
	// jobPool.QueueJob("main", &FolderReader{folder_base + "/" + "2017-04-16", "2017-04-16"}, false)
	// jobPool.QueueJob("main", &FolderReader{folder_base + "/" + "2017-04-13", "2017-04-13"}, false)

	//wash os//no_os
	//ReadFolder("/Users/edward/work/backup/2017-04-17")

	var input string
	fmt.Scanln(&input)
	fmt.Println("done")
}

func trafficListReport(wp *FolderReader, path string) {

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
			if TrafficList[key] == nil {
				TrafficList[key] = new(TrafficRatio)
			}
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
			if (*rl).Records[index].Campaign.Status != "noad" {
				TrafficList[key].Status++
			}
			if (*rl).Records[index].User.Lat != "" {
				TrafficList[key].Lat++
			}
			if (*rl).Records[index].User.Lon != "" {
				TrafficList[key].Lon++
			}
			if (*rl).Records[index].Campaign.Bidder == "voicead" {
				TrafficList[key].VoiceAd++
			}
			if (*rl).Records[index].Campaign.Bidder == "coolpad" {
				TrafficList[key].CoolPad++
			}
			if (*rl).Records[index].Campaign.Bidder == "hellogame" {
				TrafficList[key].HelloGame++
			}
			//TrafficList[key]

		}
		fmt.Printf("file: %s done\n", file)
	}
	//path := folder_Ouputs_no_os + wp.Date + ".csv"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Create(path)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if _, err = f.WriteString("pub_v_id, app_id, size, Counter, validAndroidId% , validCarrier_code%, validConn_type%, lat%, lon%, Smaato%, VoiceAd%, status%"); err != nil {
		panic(err)
	}
	for traffic := range TrafficList {
		heads := strings.Split(traffic, ",")
		//fmt.Println(heads[0], ", ",heads[1], ", ", heads[2], " = ", len(TrafficList[traffic]))
		//word := heads[0]+ ", "+heads[1]+ ", "+ heads[2]+ " = "+ len(TrafficList[traffic])
		//if _, err = f.WriteString("\n" + heads[0] + ", " + heads[1] + ", " + heads[2] + ", " + strconv.Itoa(TrafficList[traffic]) + ", " + wp.Date); err != nil {
		total := float64(TrafficList[traffic].Counter)
		if _, err = f.WriteString("\n" + heads[0] + ", " + heads[1] + ", " + heads[2] + ", " +
			strconv.Itoa(TrafficList[traffic].Counter) + ", " +
			strconv.FormatFloat(float64(TrafficList[traffic].Android_id)/total, 'f', 3, 64) + ", " +
			strconv.FormatFloat(float64(TrafficList[traffic].Carrier_code)/total, 'f', 3, 64) + ", " +
			strconv.FormatFloat(float64(TrafficList[traffic].Conn_type)/total, 'f', 3, 64) + ", " +
			strconv.FormatFloat(float64(TrafficList[traffic].Lat)/total, 'f', 3, 64) + ", " +
			strconv.FormatFloat(float64(TrafficList[traffic].Lon)/total, 'f', 3, 64) + ", " +
			strconv.FormatFloat(float64(TrafficList[traffic].Smaato)/total, 'f', 3, 64) + ", " +
			strconv.FormatFloat(float64(TrafficList[traffic].VoiceAd)/total, 'f', 3, 64) + ", " +
			strconv.FormatFloat(float64(TrafficList[traffic].Status)/total, 'f', 6, 64)); err != nil {
			panic(err)
		}
	}
}

func AdnReport(vadn string, folder string, Date string) {

	path := folder_Ouputs + Date + "_" + vadn + "_adnresponse.csv"
	//export result to file
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Create(path)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if _, err = f.WriteString("set, pub_v_id, app_id, bidder, camp_id, size, ext_id, time, device_mac, ios_ifa, android_id, ip\n"); err != nil {
		panic(err)
	}

	//TrafficList := make(map[string]*TrafficRatio) // int) //[]dt.Record)
	//ReadFolderBase()
	files := dt.GetFilelist(folder)
	fmt.Printf("%s files %v\n", Date, len(files))

	size := make(map[string]int)
	status := make(map[string]int)

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
			size[(*rl).Records[index].User.Size] += 1
			status[(*rl).Records[index].Campaign.Set] += 1
			i, err := strconv.ParseInt((*rl).Records[index].Campaign.Time, 10, 64)

			//(*rl).Records[index].Campaign.Set == "imps" &&
			if //(*rl).Records[index].Campaign.Status == "yesad" &&
			//(*rl).Records[index].Campaign.Bidder == vadn &&
			(*rl).Records[index].Campaign.Set == "clicks" &&
				(i >= 1494658800 && i <= 1494745200) {

				tm := time.Unix(i, 0)
				entry := (*rl).Records[index].Campaign.Set + ", " + (*rl).Records[index].Campaign.Pub_v_id + ", " +
					(*rl).Records[index].Campaign.App_id + ", " + (*rl).Records[index].Campaign.Bidder + ", " +
					(*rl).Records[index].Campaign.Camp_id + ", " + (*rl).Records[index].User.Size + ", " +
					(*rl).Records[index].Campaign.Ext_id + ", " + tm.String() + ", " +
					(*rl).Records[index].Device.Device_mac + ", " + (*rl).Records[index].Device.Ios_ifa + ", " +
					(*rl).Records[index].Device.Android_id + ", " + (*rl).Records[index].User.Ip
				if _, err = f.WriteString(entry + "\n"); err != nil {
					panic(err)
				}
			}
		}
	}
	// fmt.Println("adn_counter :=", adn_counter, " ; counter :=", over_counter)
	// for k, v := range size {
	// 	fmt.Println(k, ":", v)
	// }
	// for k, v := range status {
	// 	fmt.Println(k, ":", v)
	// }
}

func ImpsReport(vadn string, folder string, Date string) {

	path := folder_Ouputs + Date + ".csv"
	//export result to file
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Create(path)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if _, err = f.WriteString("set, pub_v_id, app_id, bidder, camp_id, size, ext_id, time, device_mac, device_type, device_model, ios_ifa, android_id, ip, lat, lon, City\n"); err != nil {
		panic(err)
	}

	//TrafficList := make(map[string]*TrafficRatio) // int) //[]dt.Record)
	//ReadFolderBase()
	files := dt.GetFilelist(folder)
	fmt.Printf("%s files %v\n", Date, len(files))

	size := make(map[string]int)
	status := make(map[string]int)

	adn_counter := 0
	over_counter := 0

	//location, _ := time.LoadLocation("Asia/Beijing")
	tm_max := time.Unix(1493810683, 0)
	tm_min := time.Unix(1497484800, 0)
	flag_init := true

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
			size[(*rl).Records[index].User.Size] += 1
			status[(*rl).Records[index].Campaign.Set] += 1

			// if (*rl).Records[index].Campaign.Set == "adn_responses" &&
			// 	(*rl).Records[index].Campaign.Status == "yesad" &&
			// 	(*rl).Records[index].Campaign.Bidder == vadn {
			// 	adn_counter++ //yesad counter?
			// }

			//max & min time converting only
			if flag_init {
				temp, _ := strconv.ParseInt((*rl).Records[index].Campaign.Time, 10, 64)
				tm_max = time.Unix(temp, 0)
				tm_min = time.Unix(temp, 0)

				fmt.Println("flag init: time_max:", tm_max, " ; time_min:", tm_min)
				flag_init = false
			}
			i, err := strconv.ParseInt((*rl).Records[index].Campaign.Time, 10, 64)
			tm := time.Unix(i, 0)
			if tm.After(tm_max) {
				tm_max = tm
			} else if tm.Before(tm_min) {
				tm_min = tm
			}

			if (*rl).Records[index].Campaign.App_id == "574" || (*rl).Records[index].Campaign.App_id == "575" {
				//&& (*rl).Records[index].Campaign.Camp_id == "2224" {
				//(*rl).Records[index].Campaign.Bidder == vadn {
				// i, err := strconv.ParseInt((*rl).Records[index].Campaign.Time, 10, 64)
				// tm := time.Unix(i, 0)
				//fmt.Println((*rl).Records[index].Device.Ios_ifa)
				entry := (*rl).Records[index].Campaign.App_id + ", " + //(*rl).Records[index].Campaign.Bidder + ", " +
					//(*rl).Records[index].Campaign.Set + ", " + (*rl).Records[index].Campaign.Pub_v_id + ", " +
					(*rl).Records[index].Campaign.Camp_id + ", " + //(*rl).Records[index].User.Size + ", " +
					//(*rl).Records[index].Campaign.Ext_id + ", " +
					tm.String() + ", " +
					//(*rl).Records[index].Device.Device_mac + ", " +
					(*rl).Records[index].Device.Device_type + ", " +
					//(*rl).Records[index].Device.Device_model + ", " + (*rl).Records[index].Device.Ios_ifa + ", " +
					//(*rl).Records[index].Device.Android_id + ", " +
					(*rl).Records[index].User.Ip + ", " +
					(*rl).Records[index].User.Lat + ", " + (*rl).Records[index].User.Lon + ", " +
					(*rl).Records[index].User.City
				if _, err = f.WriteString(entry + "\n"); err != nil {
					panic(err)
				}
				over_counter++ //imps counter?
			}
		}
	}
	fmt.Println("time_max:", tm_max, " ; time_min:", tm_min)
	fmt.Println("time_max:", strconv.FormatInt(tm_max.Unix(), 10), " ; time_min:", strconv.FormatInt(tm_min.Unix(), 10))
	fmt.Println("for vadn=", vadn, "adn_counter yesad:=", adn_counter, " ; total imps:=", over_counter)
	// for k, v := range size {
	// 	fmt.Println(k, ":", v)
	// }
	// for k, v := range status {
	// 	fmt.Println(k, ":", v)
	// }
}

func UnqiueUserReport(vadn string, folder string, Date string) {

	path := folder_Ouputs + Date + ".csv"
	//export result to file
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Create(path)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if _, err = f.WriteString("set, pub_v_id, app_id, bidder, camp_id, size, ext_id, time, device_mac, ios_ifa, android_id, ip\n"); err != nil {
		panic(err)
	}

	//TrafficList := make(map[string]*TrafficRatio) // int) //[]dt.Record)
	//ReadFolderBase()
	files := dt.GetFilelist(folder)
	fmt.Printf("%s files %v\n", Date, len(files))

	size := make(map[string]int)
	status := make(map[string]int)

	adn_counter := 0
	over_counter := 0
	campaign_counter := 0

	//location, _ := time.LoadLocation("Asia/Beijing")
	tm_max := time.Unix(1493810683, 0)
	tm_min := time.Unix(1497484800, 0)
	flag_init := true

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
			size[(*rl).Records[index].User.Size] += 1
			status[(*rl).Records[index].Campaign.Set] += 1

			if (*rl).Records[index].Campaign.Set == "adn_responses" &&
				(*rl).Records[index].Campaign.Status == "yesad" &&
				(*rl).Records[index].Campaign.Bidder == vadn {
				adn_counter++ //yesad counter?
			}

			if (*rl).Records[index].Campaign.Set == "imps" &&
				(*rl).Records[index].Campaign.Bidder == vadn &&
				(*rl).Records[index].Campaign.Camp_id == "2048" {
				campaign_counter++ //yesad counter?
			}

			//max & min time converting only
			if flag_init {
				temp, _ := strconv.ParseInt((*rl).Records[index].Campaign.Time, 10, 64)
				tm_max = time.Unix(temp, 0)
				tm_min = time.Unix(temp, 0)

				fmt.Println("flag init: time_max:", tm_max, " ; time_min:", tm_min)
				flag_init = false
			}
			i, err := strconv.ParseInt((*rl).Records[index].Campaign.Time, 10, 64)
			tm := time.Unix(i, 0)
			if tm.After(tm_max) {
				tm_max = tm
			} else if tm.Before(tm_min) {
				tm_min = tm
			}

			if (*rl).Records[index].Campaign.Set == "imps" &&
				(*rl).Records[index].Campaign.Bidder == vadn {
				// i, err := strconv.ParseInt((*rl).Records[index].Campaign.Time, 10, 64)
				// tm := time.Unix(i, 0)
				entry := (*rl).Records[index].Campaign.Set + ", " + (*rl).Records[index].Campaign.Pub_v_id + ", " +
					(*rl).Records[index].Campaign.App_id + ", " + (*rl).Records[index].Campaign.Bidder + ", " +
					(*rl).Records[index].Campaign.Camp_id + ", " + (*rl).Records[index].User.Size + ", " +
					(*rl).Records[index].Campaign.Ext_id + ", " +
					tm.String() + ", " +
					(*rl).Records[index].Device.Device_mac + ", " + (*rl).Records[index].Device.Ios_ifa + ", " +
					(*rl).Records[index].Device.Android_id + ", " + (*rl).Records[index].User.Ip
				if _, err = f.WriteString(entry + "\n"); err != nil {
					panic(err)
				}
				over_counter++ //imps counter?
			}
		}
	}
	fmt.Println("time_max:", tm_max, " ; time_min:", tm_min)
	fmt.Println("time_max:", strconv.FormatInt(tm_max.Unix(), 10), " ; time_min:", strconv.FormatInt(tm_min.Unix(), 10))
	fmt.Println("for vadn=", vadn, "adn_counter yesad:=", adn_counter, " ; total imps:=", over_counter)
	fmt.Println("campaign:= 2048, total imps:=", campaign_counter)
	// for k, v := range size {
	// 	fmt.Println(k, ":", v)
	// }
	// for k, v := range status {
	// 	fmt.Println(k, ":", v)
	// }
}

// func LBSReport(vadn string, folder string, Date string) {

// 	path := folder_Ouputs + Date + ".csv"
// 	//export result to file
// 	if _, err := os.Stat(path); os.IsNotExist(err) {
// 		os.Create(path)
// 	}
// 	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer f.Close()

// 	/*
// 		csv header
// 	*/
// 	// if _, err = f.WriteString("set, pub_v_id, app_id, bidder, camp_id, size, ext_id, time, device_mac, ios_ifa, android_id, ip\n"); err != nil {
// 	// 	panic(err)
// 	// }

// 	//TrafficList := make(map[string]*TrafficRatio) // int) //[]dt.Record)
// 	//ReadFolderBase()
// 	files := dt.GetFilelist(folder)
// 	fmt.Printf("%s has %v files\n", Date, len(files))

// 	apps := make(map[int][]int)
// 	lbs_counter := 0

// 	//location, _ := time.LoadLocation("Asia/Beijing")
// 	tm_max := time.Unix(1493810683, 0)
// 	tm_min := time.Unix(1497484800, 0)
// 	flag_init := true

// 	for _, file := range files {

// 		//operate each file separately
// 		rl := &dt.RecordList{}
// 		configFile, err := os.Open(file)
// 		//Todo: extract error class
// 		if err != nil {
// 			fmt.Println("opening json file, error:", err.Error())
// 		}
// 		jsonParser := json.NewDecoder(configFile)
// 		if err = jsonParser.Decode(&rl); err != nil {
// 			fmt.Println("parsing config file, error:", err.Error())
// 		}

// 		for index := range (*rl).Records {
// 			size[(*rl).Records[index].User.Size] += 1
// 			status[(*rl).Records[index].Campaign.Set] += 1

// 			if (*rl).Records[index].User.Lat != "" && (*rl).Records[index].User.Lat != "0" {
// 				apps := make(map[int][]int)
// 				lbs_counter++
// 			}

// 			//max & min time converting only
// 			if flag_init {
// 				temp, _ := strconv.ParseInt((*rl).Records[index].Campaign.Time, 10, 64)
// 				tm_max = time.Unix(temp, 0)
// 				tm_min = time.Unix(temp, 0)

// 				fmt.Println("flag init: time_max:", tm_max, " ; time_min:", tm_min)
// 				flag_init = false
// 			}
// 			i, err := strconv.ParseInt((*rl).Records[index].Campaign.Time, 10, 64)
// 			tm := time.Unix(i, 0)
// 			if tm.After(tm_max) {
// 				tm_max = tm
// 			} else if tm.Before(tm_min) {
// 				tm_min = tm
// 			}

// 			if (*rl).Records[index].Campaign.App_id == "417" && (*rl).Records[index].Campaign.Camp_id == "2224" {
// 				//(*rl).Records[index].Campaign.Bidder == vadn {
// 				// i, err := strconv.ParseInt((*rl).Records[index].Campaign.Time, 10, 64)
// 				// tm := time.Unix(i, 0)
// 				//fmt.Println((*rl).Records[index].Device.Ios_ifa)
// 				entry := (*rl).Records[index].Campaign.Set + ", " + (*rl).Records[index].Campaign.Pub_v_id + ", " +
// 					(*rl).Records[index].Campaign.App_id + ", " + (*rl).Records[index].Campaign.Bidder + ", " +
// 					(*rl).Records[index].Campaign.Camp_id + ", " + (*rl).Records[index].User.Size + ", " +
// 					(*rl).Records[index].Campaign.Ext_id + ", " +
// 					tm.String() + ", " +
// 					(*rl).Records[index].Device.Device_mac + ", " + (*rl).Records[index].Device.Ios_ifa + ", " +
// 					(*rl).Records[index].Device.Android_id + ", " + (*rl).Records[index].User.Ip
// 				if _, err = f.WriteString(entry + "\n"); err != nil {
// 					panic(err)
// 				}
// 				over_counter++ //imps counter?
// 			}
// 		}
// 	}
// 	fmt.Println("time_max:", tm_max, " ; time_min:", tm_min)
// 	fmt.Println("time_max:", strconv.FormatInt(tm_max.Unix(), 10), " ; time_min:", strconv.FormatInt(tm_min.Unix(), 10))
// 	fmt.Println("for vadn=", vadn, "adn_counter yesad:=", adn_counter, " ; total imps:=", over_counter)
// 	fmt.Println("campaign:= 2048, total imps:=", campaign_counter)
// 	// for k, v := range size {
// 	// 	fmt.Println(k, ":", v)
// 	// }
// 	// for k, v := range status {
// 	// 	fmt.Println(k, ":", v)
// 	// }
// }
