package storage

type Storage interface {
	Insert()
	Update()
	Delete()
	Read()
	Commit()
	Abort()
}
