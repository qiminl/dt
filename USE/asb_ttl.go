package main

import (
	dt "dt"
	"fmt"
	"io/ioutil"
	//"strconv"
	"strings"
)

var (
	//directory = "/Users/edward/aerospike-vm/bu"
	directory = "/Users/edward/work/backup/2017-04-12"
)

func test_reader(files []string) {
	i := 0
	for _, file := range files {
		input, err := ioutil.ReadFile(file)
		if err != nil {
			//log.Fatalln(err)
			fmt.Println(err)
		}

		lines := strings.Split(string(input), "\n")

		for i, line := range lines {
			if strings.Contains(line, "+ t ") {
				lines[i] = "+ t 0"
			}
		}
		output := strings.Join(lines, "\n")
		err = ioutil.WriteFile(file, []byte(output), 0644)
		if err != nil {
			//log.Fatalln(err)
			fmt.Println(err)
		}
		i++
	}
}

func main() {
	test_reader(dt.GetFilelist(directory))
}
