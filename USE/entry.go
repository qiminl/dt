package main

import (
	asb_r "dt"
	"encoding/json"
	"fmt"
	"time"
)

var(
	date = "2016-12-01"
	folder_base ="/Users/edward/work/backup/"
	folder = folder_base+ date
)

func mash_test(){

	res2D := &asb_r.Test{
		Id: "hmm",
		Hehe: []string{"diu","ai"}}
	res2D.Hehe = append(res2D.Hehe, "ahsadf")
	res2B, _ := json.Marshal(res2D)
	fmt.Println(string(res2B))

	m2D := &[]asb_r.Test{
		asb_r.Test{"hmm",[]string{"asdf","asdf"}}, 
		asb_r.Test{"hmm",[]string{"asdf","asdf"}}}
	m2B, _ := json.Marshal(m2D)
	fmt.Println(string(m2B))

	recordList := &asb_r.TestList{
		[]asb_r.Test{ 
			{"id1",[]string{"st","ring"}}, 
			{"id2",[]string{"st","ring"}}}}
	recordList2B, _ := json.Marshal(recordList)
	fmt.Println(string(recordList2B))

	keke := []asb_r.Test{}
	keke = append(keke, asb_r.Test{"id1",[]string{"st","ring"}}, 
			asb_r.Test{"id2",[]string{"st","ring"}})

	recordList2 := &asb_r.TestList{keke}
	recordList2B2, _ := json.Marshal(recordList2)
	fmt.Println(string(recordList2B2))
}

func main(){
	//mash_test()
	var count int;
	rl := &[]asb_r.Record{}
	start := time.Now()
	files := asb_r.GetFilelist(folder)
	//fmt.Println(folder, "file # ", len(files))
	for _,file := range files{
		//imp +=  read_field(file, m, TAG)
		//imp+= read_record(file, m, TAG)
		count += asb_r.Read_Records(file, rl)
		//fmt.Println(count)
 	}
 	t2 := time.Now()
    RecordList2D := &asb_r.RecordList{*rl}
	RecordList2B, _ := json.Marshal(RecordList2D)
	fmt.Println(string(RecordList2B))


    fmt.Printf("load data time cost %v\n",t2.Sub(start)) 
    fmt.Printf("total count %d\n",count) 
}