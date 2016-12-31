package main

import (
	"dt"
	"fmt"
)

func main(){
	res2D := &Test{
		Id: "hmm",
		Hehe: []string{"diu","ai"}
	}

	res2B, _ := json.Marshal(res2D)
	fmt.Println(string(res2B))
}