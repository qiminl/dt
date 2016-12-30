package main

import (
	"fmt"
	"os"  
    "io"  
    "bufio"
    "io/ioutil"  
    TT "time"
    "strings"
    "sort"
    "path/filepath"
    "encoding/csv"
    "strconv"
    //"encoding/json"
)

var(

	unique_ifa map[string] int
	set_count map[string] int
	count int
	m map[string] int
	
	unique_record = false
	target_app_id = "160"
	target_set = "clicks"

	//app_ids = []string{"62","78", "94", "95", "96", "97", "98", "99", "100"}
	date = "2016-12-16"
	folder_base ="/Users/liuqimin/work/"
	folder = folder_base+ date
	TAG = "region"
    csv_file = "GeoLite2-City-Locations-zh-CN - GeoLite2-City-Locations-zh-CN.csv"
)
	
type Record struct{
	Id string `json:"id"`
	Set string `json:"set"`
	Time 	string `json:"time"`
	App_id 	string`json:"app_id"`
	Camp_id string`json:"camp_id"`
	Ip 		string`json:"ip"`
	Os_n	string `json:"os_v"`
	Os_v	string`json:"os_v"`
	Device_id string`json:"device_id"`
	Device_mac string`json:"device_mac"`
	Device_type string`json:"device_type"`
	Device_ifa string`json:"device_ifa"`
	City 	string`json:"city"`
}

type RecordList struct{
	Records []Record
}

// func Record_init(rl Record){
// 	rl.set = make
// }

//in order to use sort 
type Pair struct {
  Key string
  Value int
}
type PairList []Pair
func (p PairList) Len() int { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int){ p[i], p[j] = p[j], p[i] }

func convert_unix_time(u_time string){
	i, err := strconv.ParseInt(u_time, 10, 64)
    if err != nil {
        panic(err)
    }
    tm := TT.Unix(i, 0)
    fmt.Println(tm)
}

/*
 *	compare result to geo data
 *  @param file absolute path to the csv file.
 *	@param m 	map of key, value of result from asb
 *  @return 	result
 */
func compare_csv(file string, m map[string] int, TAG string) map[string] int{
	
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

//Not in use right nowRead entire file all at once.
func read3(path string)string{  
    fi,err := os.Open(path)  
    if err != nil{panic(err)}  
    defer fi.Close()  

    fd,err := ioutil.ReadAll(fi)  
    fmt.Println(string(fd))  
    return string(fd)  
}  

/*
 *	read each line of the give file.
 *  @param path absolute path to the file.
 *	@param m 	map of key, value in the file 
 *  @return 	int, the count of entries readed. 
 */
func read_field(path string, m map[string] int, TAG string) int{

	f, err := os.Open(path)
	if err != nil{panic(err)}  
	defer f.Close()
	
	count := 0

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		line := strings.Fields(scanner.Text())
		//fmt.Println(" line[0]: ", line[0])
		if len(line) >=5{
			//fmt.Println("line: ", line)
			switch{
			case TAG == "city":
			//name,country,field :=  line[2], line[3], line[4]
				name,field :=  line[2], line[4]
				if name == TAG {//&& (country == "2" || country == "cn"){
					//fmt.Println(name, country, field)
					m[field] = m[field]+1
					count++
				}
			
			case TAG == "region":
				name,country,field :=  line[2], line[3], line[4]
				if name == TAG && (country == "2" || country == "cn"){
					//fmt.Println(name, country, field)
					m[field] = m[field]+1
					count++
				}

			case TAG == "os_v":
				name,field :=  line[2], line[3]
				if name == TAG {
					//fmt.Println(name, country, field)
					m[field] = m[field]+1
					count++
				}
			}
		}
	}
	//fmt.Println("map size", len(m))
	return count
}

/*
 *	get all files in the path.
 *  @param path absolute path
 *  @return []string array of file names.
 */
func getFilelist(path string) []string{
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
        fmt.Printf("filepath.Walk() returned %v\n", err)
    }
    // for _,file := range files {
    // 	println(file)
    // }
    return files
}

/*
 *	Sort the map in decending order base on value
 *  @param m map to be sorted
 *  @return PairList list of the sorted map 
 */
func rank(m map[string] int) PairList{
  pl := make(PairList, len(m))
  i := 0
  for k, v := range m {
    pl[i] = Pair{k, v}
    i++
  }
  sort.Sort(sort.Reverse(pl))
  return pl
}

func printMap(m map[string] int){
	for key,value := range m{
       	fmt.Println("key:", key, ", value:", value )
    }
}
func printMap2(m map[int] int){
	for key,value := range m{
       	fmt.Println("key:", key, ", value:", value )
    }
}

func read_record(path string, m map[string] int, TAG string) int{

	

	app_id := "0"
	key := "0"
	//pricing_model := "CPM"
	current_set := ""
	ifa := "none"
	//value := "0"

	f, err := os.Open(path)
	if err != nil{panic(err)}  
	defer f.Close()
	
	count := 0

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		line := strings.Fields(scanner.Text())
		//fmt.Println(" line[0]: ", line[0])
		if len(line) >2{

			if line[1] == "n" && current_set == target_set{ //&& pricing_model=="CPM"

				if app_id == target_app_id {
					//fmt.Println("app_id: ", app_id)
					m[key]= m[key]+1
					unique_ifa[ifa] +=1
					count++
				}
			}
		
			if line[0] =="+" && line[1] == "s"{
				current_set = line[2]
				set_count[line[2]] += 1
			}

			if line[2] == "device_ifa"{
				if line[3] != "0"{
					ifa = line[4]
				} else{
					ifa = "0"
				}
			}

			if line[2] == "app_id"{
				app_id = line[4]
			}

			// if line[2] == "os_v" && len(line)>5{
			// 	key = line[5]
			// }
			if len(line)>=5 {
				name,country,field :=  line[2], line[3], line[4]
				if name == "region" && (country == "2" || country == "cn"){
					//fmt.Println(name, country, field)
					//m[field] = m[field]+1
					//count++
					//println(field)
					key = field
				}
			}

			// if len(line)>=5{
			// 	if line[2] == "p_m"{
			// 		pricing_model = line[4]
			// 	}
			// }

		}
	}
	
	//printMap(unique_ifa)

	return count
}

func read_records(path string, rl []Record) int{

	//pricing_model := "CPM"
	current_set := "none"
	time := "220003632"
	app_id := "none"
	camp_id := "none"
	id := "0"
	ip := "0"
	os_n := "0"
	os_v := "0"
	device_id := "0"
	device_mac := "0"
	device_type := "0"
	device_ifa := "0"
	city := "0"
	//value := "0"

	f, err := os.Open(path)
	if err != nil{panic(err)}  
	defer f.Close()
	
	
	// file, err :=os.Create("result.csv")
	// if err !=nil{panic(err)}
	// defer file.Close()
	// writer := csv.NewWriter(file)
	//fmt.Println("current_set", "time", "app_id", "os_v", "device_ifa", "camp_id", "ip", "device_id", "device_mac", "device_type", "city")
	//flag := true
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		line := strings.Fields(scanner.Text())
		//fmt.Println(" line[0]: ", line[0])
		
		if len(line) >2{
			if line[1] == "n" { //&& pricing_model=="CPM"
				// if app_id == target_app_id {//&& current_set == target_set{
				// 	if unique_record {
				// 		for errr :=1; errr<count; errr++{
				// 			if rl[errr].Id == id{
				// 				flag = false
				// 				fmt.Println(id)
				// 			}
				// 		}
				// 	}
				// 	//fmt.Println("app_id: ", app_id)
				// 	if flag {
						rl[count].Id = id
						rl[count].Set = current_set
						rl[count].Time = time
						rl[count].App_id = app_id
						rl[count].Camp_id = camp_id
						rl[count].Ip = ip
						rl[count].Os_n = os_n
	 					rl[count].Os_v = os_v
						rl[count].Device_id = device_id
						rl[count].Device_mac = device_mac
						rl[count].Device_type = device_type
						rl[count].Device_ifa = device_ifa
						rl[count].City = city
						i, err := strconv.ParseInt(time, 10, 64)
					    if err != nil {
					        panic(err)
					    }
					    //gmt, _ := TT.LoadLocation("Asia/Beijing")
					    rl[count].Time = TT.Unix(i, 0).String()//.In(gmt)
						count++
				// 	}
				// 	flag = true
				// 	// if device_ifa==target_ifa{
				// 	// 	fmt.Println(device_ifa, current_set, time)
				// 	// }
				// }
			}
			
			if line[2] == "id" {
				m[line[4]] = m[line[3]]+1
			}

			switch {
			//set
			case line[0] =="+" && line[1] == "s":
				current_set = line[2]
				set_count[line[2]] += 1
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
	
	//printMap(unique_ifa)

	return count
}


// func print_RecordList (rl Record, len int){
// 		// rl2s, _ := json.Marshal(rl)
// 		// println(rl2s)
// 	for i := 0; i<len; i++{
// 		fmt.Println (rl.Set[i])
// 	}
// }

// func read_ifa_list(){
// 	f, err := os.Open(path)
// 	if err != nil{panic(err)}  
// 	defer f.Close()
	
	
// 	// file, err :=os.Create("result.csv")
// 	// if err !=nil{panic(err)}
// 	// defer file.Close()
// 	// writer := csv.NewWriter(file)
// 	//fmt.Println("current_set", "time", "app_id", "os_v", "device_ifa", "camp_id", "ip", "device_id", "device_mac", "device_type", "city")
					
// 	scanner := bufio.NewScanner(f)
// 	for scanner.Scan() {
// }

func main(){
	// var m map[string] int
	// m = make(map[string]int)
	count = 0
	m = make (map[string]int)

	var rl []Record
	//rl = make(Record, 10000)
	rl = make([]Record, 5000000)
	unique_ifa = make(map[string]int)
	set_count = make(map[string]int)

	start := TT.Now()
	files := getFilelist(folder)
	//time the read file opertation
	
	for _,file := range files{
		//imp +=  read_field(file, m, TAG)
		//imp+= read_record(file, m, TAG)
		read_records(file, rl)
		//fmt.Println(count)
 	}
 	t2 := TT.Now()  

    fmt.Println("date: 	", date)
    fmt.Println("total data count = ", count)
    fmt.Printf("load data time cost %v\n",t2.Sub(start)) 

    //fmt.Println(rl[count-2000])

    // for i:=0;  i<count;i++{
    // 	//if (rl[i].Device_ifa == "862604039187055") || (rl[i].Device_ifa == "A1000043F6E6FD" )|| (rl[i].Device_ifa == "869805028000216") || (rl[i].Device_ifa == "866538021684891" ){
    // 		fmt.Println(rl[i])
    // 	//}
    // 	//fmt.Println(rl[i].Device_ifa)
    // }
    var mac map[string] int
    mac = make (map[string] int)
     for i:=0;  i<count;i++{
     	mac[rl[i].Device_mac] +=1
    }
    printMap(mac)
}
    
    // rl2s, _ := json.Marshal(rl)
    // fmt.Println(rl2s)


    // for k,v := range m{
    // 	if v>1{println(k)}
    // }
    //fmt.Println("app ids", app_ids)
    //fmt.Println("Chance APP ID: ", Chance_app_id)
    // if TAG != "os_v"{
    // 	m = compare_csv(csv_file, m, TAG)
    // }
    //t3 := time.Now()  
    //fmt.Printf("find region time cost%v\n",t3.Sub(t2)) 

//     //printMap(m)
    //pl := rank(m)
    //fmt.Println("@", date, " 我们的imp总数是", imp)
//    fmt.Println("在中国境内分布是:")
    // for key,value := range pl{
    // 	fmt.Println("key:", key, ", value:", value )
    // }

    //print_RecordList (rl, imp)

    // printMap(set_count)
    // fmt.Println("unique_ifa size", len(unique_ifa))
    // printMap(unique_ifa)