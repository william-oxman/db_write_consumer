package db

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

// NewMySQLDBClient creates a mysql DB connection
func NewMySQLDBClient(User string, dbPassword string, Host string, Port int, Database string) (*sql.DB, error) {
	// mysql connection cfg
	cfg := mysql.Config{
		User:                 User,
		Passwd:               dbPassword,
		Net:                  "tcp",
		Addr:                 "127.0.0.1:3306",
		DBName:               "testbase",
		AllowNativePasswords: true,
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(100)

	return db, nil
}
