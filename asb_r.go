package dt

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
