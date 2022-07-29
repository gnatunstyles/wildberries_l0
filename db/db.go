package db

import (
	"database/sql"
	"fmt"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "db"
)

var (
	DB *sql.DB
)

func InitDB() (*sql.DB, error) {
	//db init
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("Error. Cannot reach db")
	}

	//Pinging db
	err = db.Ping()
	fmt.Println("db connected")
	if err != nil {
		return nil, fmt.Errorf("Error. Cannot ping db: %s", err)
	}
	fmt.Println("db pinged")
	return db, nil

}
