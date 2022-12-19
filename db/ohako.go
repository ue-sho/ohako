package db

type Ohako struct {
	Name string
}

func NewDB() (*Ohako, error) {
	return &Ohako{Name: "ohako db"}, nil
}
