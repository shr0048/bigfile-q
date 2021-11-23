package driver

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
)

type SQLinfo struct {
	SQLClient *sql.DB
}

// InitSQL : Initialize SQL driver
func (mySQL *SQLinfo) InitSQL() {
	client, err := sql.Open("sqlite3","temp.db")
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("Connected to SQLITE3!")
		mySQL.SQLClient = client
	}
}
