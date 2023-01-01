package command

import (
	"github.com/ue-sho/ohako/storage"
)

type CommandResponse struct {
	Message string
	IsQuit  bool
}

type Command interface {
	Execute(s storage.Storage) CommandResponse
}

type InsertCommand struct {
	args []string
}

func (ic *InsertCommand) Execute(s storage.Storage) CommandResponse {

	return CommandResponse{Message: "insert", IsQuit: false}
}

type UpdateCommand struct {
	args []string
}

func (uc *UpdateCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "update", IsQuit: false}
}

type DeleteCommand struct {
	args []string
}

func (dc *DeleteCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "delete", IsQuit: false}
}

type ReadCommand struct {
	args []string
}

func (rc *ReadCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "read", IsQuit: false}
}

type CommitCommand struct {
	args []string
}

func (cc *CommitCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "commit", IsQuit: false}
}

type AbortCommand struct {
	args []string
}

func (ac *AbortCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "abort", IsQuit: false}
}

type QuitCommand struct {
	args []string
}

func (qc *QuitCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "Terminate", IsQuit: true}
}

type InvalidCommand struct {
	args []string
}

func (ivc *InvalidCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "Command is invalid.", IsQuit: false}
}
