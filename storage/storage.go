package storage

type Storage interface {
	Initialize() error
	Insert(record Record) error
	Update(record Record) error
	Delete(key string) error
	Read(key string) ([]byte, error)
	Commit() error
	Abort() error
}
