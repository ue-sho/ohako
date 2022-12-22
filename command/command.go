package command

import "github.com/ue-sho/ohako/storage"

type Command interface {
	Execute(s storage.Storage)
}

type InsertCommand struct{}

func (ic *InsertCommand) Execute(s storage.Storage) {

}

type UpdateCommand struct{}

func (uc *UpdateCommand) Execute(s storage.Storage) {

}

type DeleteCommand struct{}

func (dc *DeleteCommand) Execute(s storage.Storage) {

}

type ReadCommand struct{}

func (rc *ReadCommand) Execute(s storage.Storage) {

}

type CommitCommand struct{}

func (cc *CommitCommand) Execute(s storage.Storage) {

}

type AbortCommand struct{}

func (ac *AbortCommand) Execute(s storage.Storage) {

}

type QuitCommand struct{}

func (qc *QuitCommand) Execute(s storage.Storage) {

}
