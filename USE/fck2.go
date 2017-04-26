package main

import (
	//dt "dt"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	path := "/Users/edward/work/backup/2017-04-16/BB9B098D7A2CE06_00000.asb"
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	//head_flag := false
	scanner := bufio.NewScanner(f)
	counter := 0
	for scanner.Scan() {
		//fmt.Println(scanner.Text())

		line := strings.Fields(scanner.Text())
		counter++
		if len(line) > 2 {
			if line[2] == "size" && len(line) > 4 {
				if !strings.Contains(line[4], "x") {
					fmt.Println("diu", line[4])
				} else {
					//fmt.Println("hmm", line[4])
				}
				//fmt.Println(line[4])
			}

		}
	}
	fmt.Println(counter)

}
