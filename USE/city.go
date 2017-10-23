package main

import (
	"encoding/csv"
	"os"
	"strconv"
	"fmt"
	"sort"
	// "io/ioutil"
	// //"strconv"
	// "strings"
)


type Pair struct {
	Key string
	Value int
}
type PairList []Pair

func SortMap(city_code map[string]int) PairList{
	pl := make(PairList, len(city_code))
	i := 0
	for k, v := range city_code {
	  pl[i] = Pair{k, v}
	  i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}
func (p PairList) Len() int { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int){ p[i], p[j] = p[j], p[i] }

type GeoRecord struct{
	Ip string 
	City string
	Lat string
	Lon string
}
var (
	date string 
	//directory = "/Users/edward/aerospike-vm/bu"
	directory = "/Users/liuqimin/Desktop/"
	geodb_dir = "/Users/liuqimin/Desktop/GeoLite2-City-CSV_20171003/"
	city_code map[string]int
)

func ReadGeoReport(path string, city_code map[string]int) {
	csvfile, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer csvfile.Close()


	reader := csv.NewReader(csvfile)
	reader.FieldsPerRecord = -1
	rawCSVdata, err := reader.ReadAll()

	if err != nil {
			fmt.Println(err)
			os.Exit(1)
	}

	// now, safe to move raw CSV data to struct
	var oneRecord GeoRecord
	var allRecords []GeoRecord

	for _, each := range rawCSVdata {
			oneRecord.Ip = each[0]
			oneRecord.City = each[1]
			oneRecord.Lat = each[2]
			oneRecord.Lon = each[3]
			allRecords = append(allRecords, oneRecord)

			city_code[oneRecord.City] +=1
	}
}

func ReplaceCode (path string, city_code map[string]int){
	csvfile, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer csvfile.Close()

	reader := csv.NewReader(csvfile)
	reader.FieldsPerRecord = -1
	rawCSVdata, err := reader.ReadAll()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("test")
	for _, each := range rawCSVdata {
		//test := each[0] +"t"
		//fmt.Println(test)
		for k, v := range city_code{
			
			if k == each[0]{
				city_code[each[7]+"-"+each[10]] = v 
				delete(city_code, k);
			}
		}
	}
}

func ExportFile (city_path string, city_code map[string]int){
	if _, err := os.Stat(city_path); os.IsNotExist(err) {
		os.Create(city_path)
	}
	f_geo, err := os.OpenFile(city_path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f_geo.Close()
	if _, err = f_geo.WriteString("city, count"); err != nil {
		panic(err)
	}
	for key, value := range city_code {
		if _, err = f_geo.WriteString("\n" + key + "," + strconv.Itoa(value)); err != nil {
			panic(err)
		}
	}
}

func ExportPair (city_path string, city_code PairList){
	if _, err := os.Stat(city_path); os.IsNotExist(err) {
		os.Create(city_path)
	}
	f_geo, err := os.OpenFile(city_path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f_geo.Close()
	if _, err = f_geo.WriteString("city, count"); err != nil {
		panic(err)
	}
	for _,each := range city_code {
		if _, err = f_geo.WriteString("\n" + each.Key + "," + strconv.Itoa(each.Value)); err != nil {
			panic(err)
		}
	}
}

func main() {
	//get date for use;
	if len(os.Args) >= 1 {
		date = os.Args[1]
		//folder_Ouputs = os.Args[2]
	} else {
		// panic(fmt.Println("Need date as argument in mm-dd format"))
		return
	}
	//city code map
	city_code = make(map[string]int)
	path := directory + date + "-report/" + date +"_geo.csv"
	ReadGeoReport(path, city_code)

	geodb_path := geodb_dir + "GeoLite2-City-Locations-zh-CN.csv"
	ReplaceCode(geodb_path, city_code)

	pairList := SortMap(city_code)
	//Print geo data
	city_path := directory + date + "-report/" + date + "_citycode.csv"
	ExportPair(city_path, pairList)
}
