package command

func ParseCommand(input string) Command {
	switch input {
	case "insert":
		return &InsertCommand{}
	case "update":
		return &UpdateCommand{}
	case "delete":
		return &DeleteCommand{}
	case "read":
		return &ReadCommand{}
	case "commit":
		return &CommitCommand{}
	case "abort":
		return &AbortCommand{}
	case "q", "quit":
		return &QuitCommand{}
	default:
		return &InvalidCommand{}
	}
}
