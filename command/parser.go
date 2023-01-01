package command

func ParseCommand(input []string) Command {
	switch input[0] {
	case "insert":
		return &InsertCommand{input}
	case "update":
		return &UpdateCommand{input}
	case "delete":
		return &DeleteCommand{input}
	case "read":
		return &ReadCommand{input}
	case "commit":
		return &CommitCommand{input}
	case "abort":
		return &AbortCommand{input}
	case "q", "quit":
		return &QuitCommand{input}
	default:
		return &InvalidCommand{input}
	}
}
