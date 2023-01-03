package db

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/ue-sho/ohako/command"
	"github.com/ue-sho/ohako/storage"
)

type Ohako struct {
	storage storage.Storage
}

func NewDB() (*Ohako, error) {
	storage := storage.NewInMemoryStorage()
	return &Ohako{storage: storage}, nil
}

func (o *Ohako) initialize() error {
	o.storage.Initialize()
	fmt.Println("ohako DB start")
	return nil
}

func (o *Ohako) Run() {
	o.initialize()
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(">> ")
		scanner.Scan()
		input := strings.Split(scanner.Text(), " ")
		comannd := command.ParseCommand(input)
		ret := comannd.Execute(o.storage)
		fmt.Println(ret.Message)
		if ret.IsQuit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Scanner error:", err)
		return
	}
}
