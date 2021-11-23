package main

import (
	"fmt"
	"github.com/shr0048/bigfile-q/driver"
	"github.com/shr0048/bigfile-q/handler"
	"github.com/shr0048/gocsv"
)

var Sql driver.SQLinfo

func main()  {
	fmt.Println("### Bigfile-Q ###")
	Sql.InitSQL()

	filePath := "meta.csv"
	sep := "\t"

	mycsv := gocsv.CSV{}
	err := mycsv.LoadCSV(filePath, sep, 2)
	if err != nil {
		fmt.Println("Error load csv file")
		fmt.Println(err)
	}

	fmt.Println("[Header of CSV]")
	for _, column := range mycsv.Header {
		fmt.Println(column)
	}

	h := &handler.Handler{MySql: Sql, MyFile: mycsv}

	err = h.LoadFileToSQLite()
	if err != nil {
		fmt.Println(err)
	}

	err = h.InsertRows()
	if err != nil {
		fmt.Println(err)
	}
}