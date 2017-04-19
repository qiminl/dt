// package dt

// import (
// 	"bufio"
// 	"encoding/csv"
// 	"fmt"
// 	"io"
// 	"os"
// 	"path/filepath"
// 	"strings"
// 	// "sort"
// 	"io/ioutil"
// 	// TT "time"
// 	"encoding/json"
// 	"strconv"
// )

// /*
//  *	compare result to geo data
//  *  @param file absolute path to the csv file.
//  *	@param m 	map of key, value of result from asb
//  *  @return 	result
//  */
// func Compare_csv(file string, m map[string]int, TAG string) map[string]int {

// 	var currunt, result map[string]int
// 	currunt = make(map[string]int)
// 	result = make(map[string]int)

// 	//copy to new
// 	for k, v := range m {
// 		currunt[k] = v
// 	}

// 	f, err := os.Open(file)
// 	if err != nil {
// 		return nil
// 	}
// 	defer f.Close()

// 	csvr := csv.NewReader(f)
// 	for {
// 		row, err := csvr.Read()
// 		if err != nil {
// 			if err == io.EOF {
// 				err = nil
// 				println("EOF")
// 				return result
// 			}
// 		}

// 		switch {
// 		case TAG == "region":

// 			if len(row) > 7 && len(currunt) > 0 {
// 				for k, v := range currunt {
// 					if row[4] == "CN" && row[6] == k {
// 						region := row[7]
// 						result[region] = v
// 						delete(currunt, k)
// 						//printMap(result)
// 						break
// 					}
// 				}
// 			}

// 		case TAG == "city":
// 			if len(row) > 7 && len(currunt) > 0 {
// 				for k, v := range currunt {
// 					if row[4] == "CN" && row[0] == k {
// 						region := row[10]
// 						result[region] = v
// 						delete(currunt, k)
// 						//printMap(result)
// 						break
// 					}
// 				}
// 			}

// 		case TAG == "os_v":
// 			if len(row) > 7 && len(currunt) > 0 {
// 				for k, v := range currunt {
// 					if row[4] == "CN" && row[0] == k {
// 						region := row[10]
// 						result[region] = v
// 						delete(currunt, k)
// 						//printMap(result)
// 						break
// 					}
// 				}
// 			}
// 		}
// 	}
// 	return result
// }

// /*
//  *	compare result to ip data
//  *  @param folder absolute path to the folder included only csv file.
//  *  @return 	result
//  */

// func Read_from_fraudlogix_csv(folder string) map[string]int {

// 	var result map[string]int
// 	// currunt = make(map[string]int)
// 	result = make(map[string]int)
// 	// //copy to new
// 	// for k,v := range m {
// 	//   currunt[k] = v
// 	// }

// 	files := GetFilelist(folder)
// 	//println(folder, "#file=", len(files))
// 	for _, file := range files {
// 		f, err := os.Open(file)
// 		if err != nil {
// 			return nil
// 		}
// 		defer f.Close()

// 		csvr := csv.NewReader(f)
// 		for {
// 			row, err := csvr.Read()
// 			if err != nil {
// 				if err == io.EOF {
// 					err = nil
// 					println("EOF")
// 					break
// 					//return result
// 				}
// 			}
// 			//println(row[0])
// 			result[row[0]] += 1
// 		}
// 	}
// 	return result
// }
