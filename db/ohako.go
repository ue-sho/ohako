package db

import (
	"github.com/ue-sho/ohako/command"
	"github.com/ue-sho/ohako/storage"
)

type Ohako struct {
	storage storage.Storage
}

func NewDB() (*Ohako, error) {
	return &Ohako{storage: &storage.InMemory{}}, nil
}

func (o *Ohako) Run() {
	comannd := command.ParseCommand()
	comannd.Execute(o.storage)
}
