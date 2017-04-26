package main 
import (
	dt "dt"
	"fmt"
	"time"
	"strings"
)

func main (){
	path:= "/Users/admin/Downloads/2017-04-17/"
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
		if len(line) > 2 {
			if line[1] == "n" {
				//size
				if (line[2] == "size" && len(line) > 4){
					if !strings.Contains(line[4], 'x') {
						fmt.Println(line[4])
			        } 
				}
			}
		}
	}
			
}