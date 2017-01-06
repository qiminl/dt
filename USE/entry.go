package main

import (
	dt "dt"
	"encoding/json"
	"fmt"
	"time"
	"bufio"
	"strings"
	"os"
	"strconv"
	"io/ioutil"
)

var(
	fraudlogix_csv = "/Users/edward/Downloads/etoron-results/medium.csv"

	count int;
	date = "2016-12-29"
	folder_base ="/Users/edward/work/backup/"
	// folder_base ="E:\\backup\\"
	folder_Ouputs = "/Users/edward/work/JsonOutputs/"
	// folder_Ouputs ="E:\\backup\\JsonOutputs\\"
	folder = folder_base+ date

	line []string 
)

func mash_test(){

	res2D := &dt.Test{
		Id: "hmm",
		Hehe: []string{"diu","ai"}}
	res2D.Hehe = append(res2D.Hehe, "ahsadf")
	res2B, _ := json.Marshal(res2D)
	fmt.Println(string(res2B))

	m2D := &[]dt.Test{
		dt.Test{"hmm",[]string{"asdf","asdf"}}, 
		dt.Test{"hmm",[]string{"asdf","asdf"}}}
	m2B, _ := json.Marshal(m2D)
	fmt.Println(string(m2B))

	recordList := &dt.TestList{
		[]dt.Test{ 
			{"id1",[]string{"st","ring"}}, 
			{"id2",[]string{"st","ring"}}}}
	recordList2B, _ := json.Marshal(recordList)
	fmt.Println(string(recordList2B))

	keke := []dt.Test{}
	keke = append(keke, dt.Test{"id1",[]string{"st","ring"}}, 
			dt.Test{"id2",[]string{"st","ring"}})

	recordList2 := &dt.TestList{keke}
	recordList2B2, _ := json.Marshal(recordList2)
	fmt.Println(string(recordList2B2))
}
func test_reader(){
	f, err := os.Open("E:\\backup\\2016-12-32\\BB92A0925AE8006_00000.asb")
	if err != nil{panic(err)}  
	defer f.Close()
	
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		line = strings.Fields(scanner.Text())
	}
    if err := scanner.Err(); err != nil {
        fmt.Fprintln(os.Stderr, err)
  	} else if !scanner.Scan() {
  		fmt.Println("eof?")
  	}
}

func read_dir_IPs(dir string){
	start := time.Now()
	hmm, _ := ioutil.ReadDir(dir)
    for _, f := range hmm {

    	rl := &[]dt.Record{}
		
    	date = f.Name()
    	folder = folder_base+ date

    	count = dt.Read_Records_From_Folder(rl, folder) 
		// RecordList2D := &dt.RecordList{*rl}
		// RecordList2B, _ := json.Marshal(RecordList2D)
		// fmt.Println(string(RecordList2B))
		//fmt.Println("count=",count)
		//fmt.Println("#elements :", (*rl)[0].Ip)

    	IPList := make (map[string][]dt.Record)
		for index := range *rl{
			IPList[(*rl)[index].Ip] = append(IPList[(*rl)[index].Ip], (*rl)[index])
		}
		fmt.Println("date:",date," has Unique IP :", len(IPList))

		// UniqueId := make (map[string][]dt.Record)
		// for index := range *rl{
		// 	UniqueId[(*rl)[index].Id] = append(UniqueId[(*rl)[index].Id], (*rl)[index])
		// }
		// fmt.Println("UniqueId :", len(UniqueId))

		IPs := make([]string, len(IPList))
		i := 0
		for k := range IPList {
		    IPs[i] = k
		    i++
		}

		folder_IPs :="/Users/edward/work/IPs/"
		dt.Write_Array(folder_IPs+date+"_"+strconv.Itoa(len(IPList))+".txt", IPs)
    }

 	t2 := time.Now()
 	fmt.Printf("load data time cost %v\n",t2.Sub(start)) 
}

func Output_Json (dir string){

	start := time.Now()
	hmm, _ := ioutil.ReadDir(dir)
	fmt.Println("Reading DIR=",dir)
    for _, f := range hmm {
	    rl := &[]dt.Record{}

		date = f.Name()
    	folder = folder_base+ date
		count = dt.Read_Records_From_Folder(rl, folder) 
		fmt.Println("rl: ", len(*rl), "; count=",count)
		dt.Write_json_Array(folder_Ouputs+date+".json", rl)

	}
	t2 := time.Now()
 	fmt.Printf("load data time cost %v\n",t2.Sub(start)) 
}

//search against fraudlogix list. 
func search_Object (dir string, csv_path string) map[string] int{

	var result_app map[string] int
	result_app = make(map[string] int)
	var result_time map[string] int
	result_time = make(map[string] int)

	var ip_list map[string] int
	ip_list = make(map[string] int)
	ip_list = dt.Read_from_fraudlogix_csv(csv_path)

	var map_problem_ip_object map[string] []*dt.Record
	map_problem_ip_object = make(map[string] []*dt.Record)

	start := time.Now()
	hmm, _ := ioutil.ReadDir(dir)
	fmt.Println("Reading DIR=",dir)
    for _, f := range hmm {
	    rl := &[]dt.Record{}

		date = f.Name()
    	folder = folder_base+ date
		count = dt.Read_Records_From_Folder(rl, folder) 
		fmt.Println("rl: ", len(*rl), "; count=",count)
		//dt.Write_json_Array(folder_Ouputs+date+".json", rl)
		for _, f := range *rl {
 			//fmt.Println(" rl.ip=", f.Ip)
 			if ip_list[f.Ip] >=1{
 				interm, _ := strconv.ParseInt(f.Time, 10, 64)
				date := time.Unix(interm, 0).String()
 				fmt.Println("catch bad ip=",f.Ip, " for app_id=",f.App_id, " at ", date)
 				map_problem_ip_object[f.Ip] = append(map_problem_ip_object[f.Ip], &f)

 				result_app[f.App_id] +=1
 				date_only := strings.Fields(date)
 				result_time[date_only[0]]+=1
 			}
 		}

	}
	t2 := time.Now()
 	fmt.Printf("load data time cost %v\n",t2.Sub(start)) 

	for k,v := range result_time{
		fmt.Println("@ ",k," there is #",v," of views&clicks with high risk")
	}

 	return result_app
}

func main(){

	//test_reader()
	//mash_test()

	//read_dir_IPs(folder_base)

	//Output_Json (folder_base)
	var result_app map[string] int
	result_app = make(map[string] int)
	result_app = search_Object(folder_base, fraudlogix_csv)

	for k,v := range result_app{
		fmt.Println("app#id=",k," has #",v," of views&clicks with ",fraudlogix_csv," risk")
	}



	// date ="2017-01-04"
	// rl := &[]dt.Record{}
	// folder = folder_base + date
	// count = dt.Read_Records_From_Folder(rl, folder) 
	// fmt.Println("rl: ", len(*rl), "; count=",count)
	// dt.Write_json_Array(folder_Ouputs+date+".json", rl)

}