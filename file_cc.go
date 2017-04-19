package dt

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	// "sort"
	"io/ioutil"
	// TT "time"
	"encoding/json"
	"strconv"
)

/*
 *	get all files in the path.
 *  @param path absolute path
 *  @return []string array of file names.
 */
func GetFilelist(path string) []string {
	files := make([]string, 0)

	//var files []string
	err := filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		//println(path)
		files = append(files, path)
		return nil
	})
	//println("diu")
	if err != nil {
		return nil
		//fmt.Printf("filepath.Walk() returned %v\n", err)
	}
	// for _,file := range files {println(file)}
	return files
}

/*
 *	Read .asb files into JSON format for our portum data structure.
 *  @param path absolute path to the asb file.
 *	@param rl 	structure of the our data record
 *  @return 	count of record
 */
func Read_Records_From_Folder(rl *[]Record, folder string) int {

	var count int
	files := GetFilelist(folder)
	println("reading from folder : ", folder)
	//println(folder, "#file=", len(files))
	for _, file := range files {
		count += Read_Records_From_File(file, rl)

		//println(file," with ",count)
	}
	return count
}

func Write_Array(path string, IPs []string) error { //m map[string] []dt.Record) {
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

func Write_json_Array(path string, rl *[]Record) { //m map[string] []dt.Record) {

	RecordList2D := &RecordList{*rl}
	RecordList2B, err := json.MarshalIndent(RecordList2D, "", "  ")
	if err != nil {
		panic(err)
	}
	//fmt.Println("Date writing to ", path, "\nRecordList.len=", len(*rl))
	//fmt.Println(string(RecordList2B))
	//fmt.Println("#elements :", (*rl)[0].Ip)

	// if _, err := os.Stat(path); err != nil {
	// 	if os.IsNotExist(err) {
	// 		os.Mkdir(path, 0755)
	// 	} else {
	// 		println(err)
	// 	}
	// }
	ioutil.WriteFile(path, RecordList2B, 0644)

	// f, err := os.Create(path)
	//    check(err)
	//    defer f.Close()
	//    w := bufio.NewWriter(f)
	//    //n4, err := w.WriteString(keys)
	//    for _, ip := range RecordList2B {
	//     fmt.Printf(w, ip)
	// }
	//  	return w.Flush()
}

func Write_json_Array_RL(path string, rl *RecordList) { //m map[string] []dt.Record) {

	RecordList2D := &rl
	RecordList2B, err := json.MarshalIndent(RecordList2D, "", "  ")
	if err != nil {
		panic(err)
	}
	ioutil.WriteFile(path, RecordList2B, 0644)
}

func Write_map_FraudLogix(result_map map[string]int, file string,
	k_description string, v_description string) {
	f, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fmt.Println(file, " created?")
	w := bufio.NewWriter(f)
	//n4, err := w.WriteString(keys)
	for k, v := range result_map {
		//fmt.Println(k_description,k," there is #",v,v_description)
		ip := k_description + k + " there is #" + strconv.Itoa(v) + v_description
		fmt.Fprintln(w, ip)
	}
	fmt.Println("Done writing ", file)
}
