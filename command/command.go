package command

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ue-sho/ohako/storage"
)

type CommandResponse struct {
	Message string
	IsQuit  bool
}

type Command interface {
	Execute(s storage.Storage) CommandResponse
}

type InsertCommand struct{}

func (ic *InsertCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "insert", IsQuit: false}
}

type UpdateCommand struct{}

func (uc *UpdateCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "update", IsQuit: false}
}

type DeleteCommand struct{}

func (dc *DeleteCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "delete", IsQuit: false}
}

type ReadCommand struct{}

func (rc *ReadCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "read", IsQuit: false}
}

type CommitCommand struct{}

func (cc *CommitCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "commit", IsQuit: false}
}

type AbortCommand struct{}

func (ac *AbortCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "abort", IsQuit: false}
}

type QuitCommand struct{}

func (qc *QuitCommand) Execute(s storage.Storage) CommandResponse {
	path := filepath.Join(".", "data.log")
	df, err := os.Create(path)
	if err != nil {
		// return err
		return CommandResponse{Message: "os.Create error", IsQuit: false}
	}
	defer func() {
		if err := df.Close(); err != nil {
			fmt.Println("defer error")
		}
	}()
	return CommandResponse{Message: "Terminate", IsQuit: true}
}

type InvalidCommand struct{}

func (ivc *InvalidCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "Command is invalid.", IsQuit: false}
}
