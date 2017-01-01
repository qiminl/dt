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
	count int;
	date = "2016-12-29"
//	folder_base ="/Users/edward/work/backup/"
	folder_base ="E:\\backup\\"
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

func Read_Records_From_Folder (rl *[]dt.Record, folder string) int{

	var count int;
	start := time.Now()
	files := dt.GetFilelist(folder)
	fmt.Println(folder, "#file=", len(files))
	for _,file := range files{
		//imp +=  read_field(file, m, TAG)
		//imp+= read_record(file, m, TAG)
		fmt.Println("file: " ,file)
		count += dt.Read_Records(file, rl)
		//fmt.Println(count)
 	}
 	t2 := time.Now()
    fmt.Printf("load data time cost %v\n",t2.Sub(start)) 
    return count
}



func main(){

	//test_reader()
	//mash_test()

	rl := &[]dt.Record{}

	folder_IPs :="E:\\backup\\IPs\\"

	hmm, _ := ioutil.ReadDir("E:\\backup\\")
    for _, f := range hmm {
    	date = f.Name()
    	folder = folder_base+ date

    	count = Read_Records_From_Folder(rl, folder) 
		// RecordList2D := &dt.RecordList{*rl}
		// RecordList2B, _ := json.Marshal(RecordList2D)
		// fmt.Println(string(RecordList2B))
		fmt.Println("count=",count)
		//fmt.Println("#elements :", (*rl)[0].Ip)

		IPList := make (map[string][]dt.Record)
		for index := range *rl{
			IPList[(*rl)[index].Ip] = append(IPList[(*rl)[index].Ip], (*rl)[index])
		}
		fmt.Println("Unique IP :", len(IPList))

		IPs := make([]string, len(IPList))
		i := 0
		for k := range IPList {
		    IPs[i] = k
		    i++
		}
		dt.Write_Array(folder_IPs+date+"_"+strconv.Itoa(len(IPList))+".txt", IPs)
    }
	// UniqueId := make (map[string][]dt.Record)
	// for index := range *rl{
	// 	UniqueId[(*rl)[index].Id] = append(UniqueId[(*rl)[index].Id], (*rl)[index])
	// }
	// fmt.Println("UniqueId :", len(UniqueId))
}