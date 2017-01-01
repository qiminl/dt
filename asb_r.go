package dt

import (
	 "os"  
     "io"  
     "bufio"
     "strings"
     "path/filepath"
     "encoding/csv"
     "fmt"
    // "sort"
    // "io/ioutil"  
    // TT "time"
    // "strconv"
    // "encoding/json"
)

type Test struct{
	Id 		string `json:"id"`
	Hehe	[]string `json:"diudiu"`
}

type TestList struct{
	Tests	[]Test `json:"tests"`
}


type Record struct{
	Id 		string `json:"id"`
	Set 	string `json:"set"`
	Time 	string `json:"time"`
	App_id 	string`json:"app_id"`
	Camp_id string`json:"camp_id"`
	Ip 		string`json:"ip"`
	Os_n	string `json:"os_v"`
	Os_v	string`json:"os_v"`
	Device_id 	string`json:"device_id"`
	Device_mac 	string`json:"device_mac"`
	Device_type string`json:"device_type"`
	Device_ifa 	string`json:"device_ifa"`
	City 		string`json:"city"`
}

type RecordList struct{
	Records []Record
}

var(

	unique_ifa map[string] int
	set_count map[string] int
	m map[string] int
	
	unique_record = false
	target_app_id = "160"
	target_set = "clicks"

	//app_ids = []string{"62","78", "94", "95", "96", "97", "98", "99", "100"}
	// date = "2016-12-16"
	// folder_base ="/Users/liuqimin/work/"
	// folder = folder_base+ date
	TAG = "region"
    csv_file = "GeoLite2-City-Locations-zh-CN - GeoLite2-City-Locations-zh-CN.csv"
)

func check(e error) {if e != nil {panic(e)}}

/*
 *	get all files in the path.
 *  @param path absolute path
 *  @return []string array of file names.
 */
func GetFilelist(path string) []string{
	files := make([]string, 0)
	
	//var files []string
    err := filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
        if ( f == nil ) {return err}
        if f.IsDir() {return nil}
        //println(path)
        files = append(files, path)
        return nil
    })
    println("diu")
    if err != nil {
    	return nil
        //fmt.Printf("filepath.Walk() returned %v\n", err)
    }
    // for _,file := range files {println(file)}
    return files
}

/*
 *	compare result to geo data
 *  @param file absolute path to the csv file.
 *	@param m 	map of key, value of result from asb
 *  @return 	result
 */
func Compare_csv(file string, m map[string] int, TAG string) map[string] int{
	
	var currunt, result map[string] int
	currunt = make(map[string]int)
	result = make(map[string]int)
	
	//copy to new
	for k,v := range m {
	  currunt[k] = v
	}

	f, err := os.Open(file)
    if err != nil {
        return nil
    }
    defer f.Close()

    csvr := csv.NewReader(f)
    for {
        row, err := csvr.Read()
        if err != nil {
            if err == io.EOF {
                err = nil
                println("EOF")
                return result 
            }
        }


		switch {
    	case TAG == "region":

	        if len(row) >7 && len(currunt) >0{
	    		for k,v := range currunt{
	    			if row[4] == "CN" && row[6] == k{
	    				region := row[7]
	    				result[region] = v
	    				delete(currunt, k)
	    				//printMap(result)
	    				break
	    			}
	    		}
	    	}
	    
	    case TAG == "city" :
	    	if len(row) >7 && len(currunt) >0{
	    		for k,v := range currunt{
	    			if row[4] == "CN" && row[0] == k{
	    				region := row[10]
	    				result[region] = v
	    				delete(currunt, k)
	    				//printMap(result)
	    				break
	    			}
	    		}
	    	}
	    

	    case TAG == "os_v" :
	    	if len(row) >7 && len(currunt) >0{
	    		for k,v := range currunt{
	    			if row[4] == "CN" && row[0] == k{
	    				region := row[10]
	    				result[region] = v
	    				delete(currunt, k)
	    				//printMap(result)
	    				break
	    			}
	    		}
	    	}
		}
	}
	return result
}

/*
 *	Read .asb files into JSON format for our portum data structure. 
 *  @param path absolute path to the asb file.
 *	@param rl 	structure of the our data record
 *  @return 	count of record
 */

func Read_Records(path string, rl *[]Record) int{
	count := 0
	//pricing_model := "CPM"
	var current_set, time, app_id, camp_id, id, ip, device_ifa,
	os_n, os_v, device_id, device_mac, device_type, city string

	f, err := os.Open(path)
	if err != nil{panic(err)}  
	defer f.Close()
	
	head_flag := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		line := strings.Fields(scanner.Text())
		//fmt.Println(" line[0]: ", line[0])
		if len(line) >2{
			if line[1] == "n" { 
				//i, _ := strconv.ParseInt(time, 10, 64)
				//time := TT.Unix(i, 0).String()
				//build a record
				if head_flag{
					*rl = append(*rl, Record{Id:id, Set:current_set, Time: time, App_id:app_id, 
						Camp_id:camp_id, Ip:ip, Os_n:os_n, Os_v:os_v, Device_id:device_id, 
						Device_mac:device_mac, Device_type:device_type, Device_ifa:device_ifa, City:city})
					count++
				} else{
					head_flag = true
				} 
			}
			
			switch {
			//set
			case line[0] =="+" && line[1] == "s":
				current_set = line[2]
				//set_count[line[2]] += 1
			//id
			case line[2] == "id":
				id = line[4]
			//time
			case line[2] == "t":
				time = line[3]
			
			//app_id
			case line[2] == "app_id" && len(line)>4:
				app_id = line[4]
			
			//camp_id
			case line[2] =="camp_id" && len(line)>4:
				camp_id = line[4]
			
			//ip
			case line[2]== "ip":
				if len(line) >4{ip = line[4]}else { ip= line[3]}
			//ip
			case line[2] =="os_n" && len(line)>3 && line[3] !="0":
				os_v = line[3]
			//os_v
			case line[2] =="os_v" && len(line)>4 && line[3] !="0":
				os_v = line[4]
			//device_id
			case line[2] =="device_id"&& len(line)>4 :
				device_id = line[4]
			//device_mac
			case line[2]== "device_mac" && len(line)>4 :
				device_mac = line[4]
			//device_type
			case line[2]== "device_type" && len(line)>4 :
				device_type = line[4]
			//device_ifa
			case line[2]== "device_ifa" && len(line)>4 :
				device_ifa = line[4]
			}
			
			if len(line)>=5 {
				name,country,field :=  line[2], line[3], line[4]
				if name == "region" && (country == "2" || country == "cn"){
					city = field
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
        println(os.Stderr, err)
  	} else if !scanner.Scan() {
  		if head_flag{
			*rl = append(*rl, Record{Id:id, Set:current_set, Time: time, App_id:app_id, 
				Camp_id:camp_id, Ip:ip, Os_n:os_n, Os_v:os_v, Device_id:device_id, 
				Device_mac:device_mac, Device_type:device_type, Device_ifa:device_ifa, City:city})
			count++
		} 
  	}
	// fmt.Println("len:",len(rl))
	// fmt.Println("id:",rl[len(rl)-1].Id)
	//printMap(unique_ifa)
	return count
}

func Write_Array(path string, IPs []string) error{//m map[string] []dt.Record) {
	f, err := os.Create(path)
    check(err)
    defer f.Close()
    w := bufio.NewWriter(f)
    //n4, err := w.WriteString(keys)
    for _, ip := range IPs {
	    fmt.Fprintln(w, ip)
	}
  	return w.Flush()
}

//testing 
func Siudiu() int{return 1}
