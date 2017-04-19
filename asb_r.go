package dt

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	//"path/filepath"
	// "sort"
	//"io/ioutil"
	// TT "time"
	//"encoding/json"
	//"strconv"
	//"io"
)

type Test struct {
	Id   string   `json:"id"`
	Hehe []string `json:"diudiu"`
}

type TestList struct {
	Tests []Test `json:"tests"`
}

type Record struct {

	//Camp_Struct
	Campaign Campaign `json:"campaign"`

	//Device_Struct
	Device Device `json:"device"`

	//User_Struct
	User User `json:"user"`
}

type RecordList struct {
	Records []Record
}

// type IP_struct struct {
// 	Ip string
// }

var (
	unique_ifa map[string]int
	set_count  map[string]int
	m          map[string]int

	unique_record = false
	target_app_id = "160"
	target_set    = "clicks"

	// app_ids = []string{"62","78", "94", "95", "96", "97", "98", "99", "100"}
	// date = "2016-12-16"
	// folder_base ="/Users/liuqimin/work/"
	// folder = folder_base+ date
	TAG      = "region"
	csv_file = "GeoLite2-City-Locations-zh-CN - GeoLite2-City-Locations-zh-CN.csv"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

//testing
func Siudiu() int { return 1 }

/*
 *	Read .asb files into JSON format for our portum data structure.
 *  @param path absolute path to the asb file.
 *	@param rl 	structure of the our data record
 *  @return 	count of record
 */
func Read_Records_From_File(path string, rl *[]Record) int {
	count := 0

	//campaign info:
	var id, set, time, app_id, camp_id, pub_id, pub_v_id, status, ext_id, bidder, cr_type string
	//user info:
	var ip, city, android_ifa, size string
	//device info:
	var os_n, os_v, device_id, device_mac, device_type, device_ifa, device_vendor, device_model, carrier_code string

	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	head_flag := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		line := strings.Fields(scanner.Text())
		//fmt.Println(" line[0]: ", line[0])
		if len(line) > 2 {
			if line[1] == "n" {
				//i, _ := strconv.ParseInt(time, 10, 64)
				//time := TT.Unix(i, 0).String()
				//build a record
				if head_flag {
					//if status == "yesad" {
					Campaign := &Campaign{Id: id, Set: set, Time: time,
						App_id: app_id, Camp_id: camp_id, Pub_id: pub_id, Pub_v_id: pub_v_id,
						Status: status, Ext_id: ext_id, Bidder: bidder, Cr_type: cr_type}

					User := &User{Ip: ip, City: city, Android_ifa: android_ifa, Size: size}

					Device := &Device{Os_n: os_n, Os_v: os_v, Device_id: device_id, Device_mac: device_mac,
						Device_type: device_type, Device_ifa: device_ifa, Device_vendor: device_vendor,
						Device_model: device_model, Carrier_code: carrier_code}

					*rl = append(*rl, Record{Campaign: *Campaign, User: *User, Device: *Device})

					device_model = ""
					count++
					//}
				} else {
					head_flag = true
				}
			}

			switch {
			//set
			case line[0] == "+" && line[1] == "s":
				set = line[2]
				//set_count[line[2]] += 1
			//id
			case line[2] == "id":
				id = line[4]
			//time
			case line[2] == "t":
				time = line[3]

			//app_id
			case line[2] == "app_id" && len(line) > 4:
				app_id = line[4]

			//camp_id
			case line[2] == "camp_id" && len(line) > 4:
				camp_id = line[4]

			//ip
			case line[2] == "ip":
				if len(line) > 4 {
					ip = line[4]
				} else {
					ip = line[3]
				}
			//ip
			case line[2] == "os_n" && len(line) > 3 && line[3] != "0":
				os_v = line[3]
			//os_v
			case line[2] == "os_v" && len(line) > 4 && line[3] != "0":
				os_v = line[4]
			//device_id
			case line[2] == "device_id" && len(line) > 4:
				device_id = line[4]
			//device_mac
			case line[2] == "device_mac" && len(line) > 4:
				device_mac = line[4]
			//device_type
			case line[2] == "device_type" && len(line) > 4:
				device_type = line[4]
			//device_ifa
			case line[2] == "device_ifa" && len(line) > 4:
				device_ifa = line[4]
			//device_vendor
			case line[2] == "device_vendor" && len(line) > 4:
				device_vendor = line[4]
			//device_vendor
			case line[2] == "ext_id" && len(line) > 4:
				ext_id = line[4]
			//device_vendor
			case line[2] == "status" && len(line) > 4:
				status = line[4]
			//device_model
			case line[2] == "device_model" && len(line) > 4:
				for loo := 3; loo < len(line); loo++ {
					device_model += line[loo]
				}
			//gender
			// case line[2]== "gender" && len(line)>4 :
			// 		gender = line[3]
			//carrier_code
			case line[2] == "carrier_code" && len(line) > 3:
				carrier_code = line[3]

			//android_ifa
			case line[2] == "android_ifa" && len(line) > 3:
				android_ifa = line[3]

			//pub_id
			case line[2] == "pub_id" && len(line) > 3:
				pub_id = line[3]

			//pub_v_id
			case line[2] == "pub_v_id" && len(line) > 3:
				android_ifa = line[3]

			//bidder
			case line[2] == "bidder" && len(line) > 3:
				bidder = line[3]

			//cr_type
			case line[2] == "cr_type" && len(line) > 3:
				cr_type = line[3]
			}

			if len(line) >= 5 {
				name, country, field := line[2], line[3], line[4]
				if name == "region" && (country == "2" || country == "cn") {
					city = field
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		println(os.Stderr, err)
	} else if !scanner.Scan() {
		if head_flag {
			//if status == "yesad" {
			Campaign := &Campaign{Id: id, Set: set, Time: time,
				App_id: app_id, Camp_id: camp_id, Pub_id: pub_id, Pub_v_id: pub_v_id,
				Status: id, Ext_id: ext_id, Bidder: bidder, Cr_type: cr_type}

			User := &User{Ip: ip, City: city, Android_ifa: android_ifa, Size: size}

			Device := &Device{Os_n: os_n, Os_v: os_v, Device_id: device_id, Device_mac: device_mac,
				Device_type: device_type, Device_ifa: device_ifa, Device_vendor: device_vendor,
				Device_model: device_model, Carrier_code: carrier_code}

			*rl = append(*rl, Record{Campaign: *Campaign, User: *User, Device: *Device})

			count++
			//}
		}
	}
	// fmt.Println("len:",len(rl))
	// fmt.Println("id:",rl[len(rl)-1].Id)
	//printMap(unique_ifa)
	return count
}

func Print_map(result_map map[string]int) {
	for k, v := range result_map {
		fmt.Println("@ ", k, " #", v)
	}
}
