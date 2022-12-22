package main

import (
	"github.com/ue-sho/ohako/db"
)

func main() {
	db, err := db.NewDB()
	if err != nil {
		return
	}
	db.Run()
}
