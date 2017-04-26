package main

import (
	dt "dt"
	"fmt"
	"time"
)

func main() {

	rl := &[]dt.Record{}
	start := time.Now()
	fmt.Println("start reading")
	count := dt.Read_Records_From_File("/Users/edward/work/backup/2017-04-17/BB9B098D7A2CE06_00001.asb", rl)
	fmt.Println("finished reading, count", count)

	t2 := time.Now()
	fmt.Printf("rw file took %v\n", t2.Sub(start))
	//for index := range *rl {
	//fmt.Printf("id: %s, size %s", (*rl)[1].Campaign.Id, (*rl)[1].User.Size)
	//}
}
