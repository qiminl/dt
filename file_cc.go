package dt

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	// "sort"
	"io/ioutil"
	// TT "time"
	"bytes"
	"encoding/json"
	"math"
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

const chunkSize int64 = 4 << 25
const objectSize int64 = 4 << 10

/**
Split large file, output to folder
@param file_path absolute file path
@param dest_folder output folder location
*/
func File_Split(file_path string, dest_folder string) bool {

	fileInfo, err := os.Stat(file_path)
	if err != nil {
		fmt.Println(err)
	}
	num := int64(math.Ceil(float64(fileInfo.Size()) / float64(chunkSize-objectSize)))
	fmt.Println("Total Chunks# ", num)

	fi, err := os.OpenFile(file_path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		fmt.Println(err)
		return false
	}

	b := make([]byte, chunkSize)
	var i int64 = 1
	var read_mark int64 = 0   //byte marker for next read
	var buffer_mark int64 = 0 //buffer marker for current chunck
	for ; i <= int64(num); i++ {

		if fileInfo.Size()-read_mark > chunkSize {
			buffer_mark = read_mark + chunkSize - objectSize
			fi.Seek(buffer_mark, 0)
			buffer := make([]byte, objectSize)
			fi.Read(buffer)
			hehe := []byte("+ n adserver-dev")
			buffer_mark = buffer_mark + int64(bytes.LastIndex(buffer, hehe))
			// if i == 1 {
			// 	fmt.Println("b4 tune: ", buffer_mark, " buffer:", string(buffer[:len(buffer)]))
			// 	fmt.Println("after tune: ", string(buffer[:bytes.LastIndex(buffer, hehe)]))
			// }
			b = make([]byte, buffer_mark-read_mark)
		} else if fileInfo.Size()-read_mark > 0 {
			b = make([]byte, fileInfo.Size()-read_mark)
		} else {
			fmt.Println("oh fck my life")
			b = make([]byte, 0)
		}

		//fmt.Println("hm:", (i-1)*(chunkSize))
		fi.Seek(read_mark, 0)
		fi.Read(b)

		read_mark = buffer_mark

		CheckFolder(dest_folder)
		f, err := os.OpenFile(dest_folder+fileInfo.Name()+"_"+strconv.Itoa(int(i))+".asb", os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			fmt.Println(err)
			return false
		}
		f.Write(b)
		f.Close()
	}
	fi.Close()
	if err != nil {
		return false
	}
	return true
}

func CheckFolder(folder string) bool {
	if _, err := os.Stat(folder); os.IsNotExist(err) {
		os.Mkdir(folder, os.ModePerm)
		fmt.Println("created: " + folder)
		return true
	} else {
		return false
	}
}
