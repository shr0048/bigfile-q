package main

import (
	"fmt"
	"github.com/shr0048/bigfile-q/driver"
	"github.com/shr0048/bigfile-q/handler"
	"github.com/shr0048/gocsv"
	"os"
	"strconv"
)

var Sql driver.SQLinfo

func main()  {
	fmt.Println("### Bigfile-Q ###")
	Sql.InitSQL()

	filePath := os.Args[1]
	sep := os.Args[2]
	headerIdx, _ := strconv.Atoi(os.Args[3])
	switch sep {
	case "tab":
		sep = "\t"
	case "comma":
		sep = ","
	case "semicolon":
		sep = ";"
	default:
		sep = ","
	}

	continuous := os.Args[4]

	mycsv := gocsv.CSV{}
	err := mycsv.LoadCSV(filePath, sep, headerIdx)
	if err != nil {
		fmt.Println("Error load csv file")
		fmt.Println(err)
	}

	fmt.Println("[Header of CSV]")
	for _, column := range mycsv.Header {
		fmt.Println(column)
	}

	h := &handler.Handler{MySql: Sql, MyFile: mycsv}

	err = h.LoadFileToSQLite(continuous)
	if err != nil {
		fmt.Println(err)
	}

	err = h.InsertRows()
	if err != nil {
		fmt.Println(err)
	}
}