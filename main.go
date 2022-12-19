package main

import (
	"fmt"

	"github.com/ue-sho/ohako/db"
)

func main() {
	db, err := db.NewDB()
	if err != nil {
		return
	}
	fmt.Println(db.Name)
}
