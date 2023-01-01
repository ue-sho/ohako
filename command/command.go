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
	if len(ic.args) != 3 {
		return CommandResponse{Message: "The number of arguments is invalid.\nusage: insert key value", IsQuit: false}
	}
	err := s.Insert(storage.Record{Key: ic.args[1], Value: []byte(ic.args[2])})
	if err != nil {
		return CommandResponse{Message: "Failed insert command", IsQuit: false}
	}
	return CommandResponse{Message: "Successful insert command", IsQuit: false}
}

type UpdateCommand struct {
	args []string
}

func (uc *UpdateCommand) Execute(s storage.Storage) CommandResponse {
	if len(uc.args) != 3 {
		return CommandResponse{Message: "The number of arguments is invalid.\nusage: update key value", IsQuit: false}
	}
	err := s.Update(storage.Record{Key: uc.args[1], Value: []byte(uc.args[2])})
	if err != nil {
		return CommandResponse{Message: "Failed update command", IsQuit: false}
	}
	return CommandResponse{Message: "Successful update command", IsQuit: false}
}

type DeleteCommand struct {
	args []string
}

func (dc *DeleteCommand) Execute(s storage.Storage) CommandResponse {
	if len(dc.args) != 2 {
		return CommandResponse{Message: "The number of arguments is invalid.\nusage: delete key", IsQuit: false}
	}
	err := s.Delete(dc.args[1])
	if err != nil {
		return CommandResponse{Message: "Failed delete command", IsQuit: false}
	}
	return CommandResponse{Message: "Successful delete command", IsQuit: false}
}

type ReadCommand struct {
	args []string
}

func (rc *ReadCommand) Execute(s storage.Storage) CommandResponse {
	if len(rc.args) != 2 {
		return CommandResponse{Message: "The number of arguments is invalid.\nusage: read key", IsQuit: false}
	}
	value, err := s.Read(rc.args[1])
	if err != nil {
		return CommandResponse{Message: "Failed read command", IsQuit: false}
	}
	return CommandResponse{Message: "value is " + string(value) + "\nSuccessful read command", IsQuit: false}
}

type CommitCommand struct {
	args []string
}

func (cc *CommitCommand) Execute(s storage.Storage) CommandResponse {
	if len(cc.args) != 1 {
		return CommandResponse{Message: "The number of arguments is invalid.", IsQuit: false}
	}
	err := s.Commit()
	if err != nil {
		return CommandResponse{Message: "Failed commit command", IsQuit: false}
	}
	return CommandResponse{Message: "Successful commit command", IsQuit: false}
}

type AbortCommand struct {
	args []string
}

func (ac *AbortCommand) Execute(s storage.Storage) CommandResponse {
	if len(ac.args) != 1 {
		return CommandResponse{Message: "The number of arguments is invalid.", IsQuit: false}
	}
	err := s.Abort()
	if err != nil {
		return CommandResponse{Message: "Failed abort command", IsQuit: false}
	}
	return CommandResponse{Message: "Successful abort command", IsQuit: false}
}

type QuitCommand struct {
	args []string
}

func (qc *QuitCommand) Execute(s storage.Storage) CommandResponse {
	if len(qc.args) != 1 {
		return CommandResponse{Message: "The number of arguments is invalid.", IsQuit: false}
	}
	return CommandResponse{Message: "Terminate", IsQuit: true}
}

type InvalidCommand struct {
	args []string
}

func (ivc *InvalidCommand) Execute(s storage.Storage) CommandResponse {
	return CommandResponse{Message: "Command is unknown.", IsQuit: false}
}
