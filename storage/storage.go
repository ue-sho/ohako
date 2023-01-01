package storage

type Storage interface {
	Insert(record Record) error
	Update(record Record) error
	Delete(key string) error
	Read(key string) ([]byte, error)
	Commit() error
	Abort() error
}
