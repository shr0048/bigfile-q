package handler

import (
	"fmt"
	"github.com/shr0048/bigfile-q/driver"
	"github.com/shr0048/gocsv"
	"reflect"
	"strings"
)

type Handler struct {
	MySql  driver.SQLinfo
	MyFile gocsv.CSV
}

func (h *Handler) LoadFileToSQLite() error {
	// Drop table first, if exist
	delQuery := "drop table if exists FileDump"
	statement, _ := h.MySql.SQLClient.Prepare(delQuery)
	res, err := statement.Exec()
	if err != nil {
		fmt.Println("Drop table error")
		fmt.Println(err)
	} else {
		fmt.Println("Table exist, drop first")
	}

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

	statement, _ = h.MySql.SQLClient.Prepare(baseQuery)
	res, err = statement.Exec()
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
	var values []string

	columnNum := h.MyFile.HeaderNum -1
	for idx, column := range h.MyFile.Header {
		if idx == columnNum {
			subQuery1 = subQuery1 + "'" + column + "'"
		} else {
			subQuery1 = subQuery1 + "'" + column + "', "
		}
	}

	row, ok := h.MyFile.NextRow()
	for ok {
		subQuery2 = subQuery2 + "("
		for idx, column := range row {
			key := reflect.ValueOf(column).MapKeys()[0].String()
			if idx == columnNum {
				subQuery2 = subQuery2 + "'" + strings.ReplaceAll(column[key], "'", "''") + "'"
			} else {
				subQuery2 = subQuery2 + "'" + strings.ReplaceAll(column[key], "'", "''") + "', "
			}
		}
		subQuery2 = subQuery2 + ")"
		values = append(values, subQuery2)

		subQuery2 = ""
		row, ok =  h.MyFile.NextRow()
	}

	query := fmt.Sprintf("INSERT INTO 'FileDump' (%s) VALUES %s", subQuery1, strings.Join(values, ","))
	fmt.Println(query)

	statement, _ := h.MySql.SQLClient.Prepare(query)
	_, err := statement.Exec()
	if err != nil {
		fmt.Println("Data insert error")
		fmt.Println(err)

		return err
	}

	return nil
}
