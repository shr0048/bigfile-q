package handler

import (
	"fmt"
	"reflect"
	"github.com/shr0048/bigfile-q/driver"
	"github.com/shr0048/gocsv"
	"strings"
)

type Handler struct {
	MySql  driver.SQLinfo
	MyFile gocsv.CSV
}

func (h *Handler) LoadFileToSQLite() error {
	baseQuery := fmt.Sprintf("create table FileDump (" +
		"id integer PRIMARY KEY autoincrement, ")

	columnNum := len(h.MyFile.Header)
	for idx, column := range h.MyFile.Header {
		if idx == columnNum-1 {
			baseQuery = baseQuery + "\"" + column + "\"" + " TEXT\n"
			baseQuery = baseQuery + ")"
		} else {
			baseQuery = baseQuery + "\"" + column + "\"" + " TEXT,\n"
		}
	}
	fmt.Println(baseQuery)

	statement, _ := h.MySql.SQLClient.Prepare(baseQuery)
	res, err := statement.Exec()
	if err != nil {
		fmt.Println("Create table error")
		fmt.Println(err)

		return err
	} else {
		fmt.Println(res.LastInsertId())
		fmt.Println(res.RowsAffected())

		return nil
	}
}

func (h *Handler) InsertRows() error {
	var subQuery1 string
	var subQuery2 string

	columnNum := h.MyFile.HeaderNum -1

	row, ok := h.MyFile.NextRow()
	for ok {
		for idx, column := range row {
			key := reflect.ValueOf(column).MapKeys()[0].String()
			if idx == columnNum {
				subQuery1 = subQuery1 + "'" + key + "'"
				subQuery2 = subQuery2 + "'" + strings.ReplaceAll(column[key], "'", "''") + "'"
			} else {
				subQuery1 = subQuery1 + "'" + key + "', "
				subQuery2 = subQuery2 + "'" + strings.ReplaceAll(column[key], "'", "''") + "', "
			}
		}

		query := fmt.Sprintf("INSERT INTO 'FileDump' (%s) VALUES (%s)", subQuery1, subQuery2)
		fmt.Println(query)

		statement, _ := h.MySql.SQLClient.Prepare(query)
		res, err := statement.Exec()
		if err != nil {
			fmt.Println("Create table error")
			fmt.Println(err)

			return err
		} else {
			fmt.Println(res.LastInsertId())
			fmt.Println(res.RowsAffected())
		}

		subQuery1 = ""
		subQuery2 = ""

		row, ok =  h.MyFile.NextRow()
	}

	return nil
}
