package main

import (
	"fmt"
	"os"
)

func main() {
	cli := NewClient()
	if err := cli.Run(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
