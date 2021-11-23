package query

import (
	"errors"
	"fmt"
	"strings"
)

type Statement struct {
	QueryString  string
	SQL          string
	Columns      []string
	QueryColumns []string
	Where        string
}

func (statement *Statement) SetQuery(query string, columns []string) error {
	statement.QueryString = query
	statement.Columns = columns

	lowerQuery := strings.ToLower(statement.QueryString)

	if strings.Contains(lowerQuery, "select") {
		statement.SQL = "SELECT"
		// Parse WHERE
		if strings.Contains(lowerQuery, "where") {
			whereIdx := strings.Index(lowerQuery, "where")
			statement.QueryColumns = strings.Split(query[7:whereIdx-1], ",")
			statement.Where = query[whereIdx+5+1:]
		} else {
			statement.Where = ""
		}

	} else {
		fmt.Println("Query Error")
		err := errors.New("NotExistQuery")

		return err
	}
	return nil
}
