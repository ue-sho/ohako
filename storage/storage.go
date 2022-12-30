package storage

type Storage interface {
	Insert(record Record)
	Update(record Record)
	Delete(key string)
	Read(key string) []byte
	Commit() error
	Abort()
}
