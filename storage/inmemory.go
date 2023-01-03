package storage

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type InMemory struct {
	storage map[string][]byte
}

func NewInMemoryStorage() *InMemory {
	return &InMemory{storage: map[string][]byte{}}
}

func (im *InMemory) Insert(record Record) error {
	if _, ok := im.storage[record.Key]; ok {
		return fmt.Errorf("%s key already in use", record.Key)
	}
	im.storage[record.Key] = record.Value
	return nil
}

func (im *InMemory) Update(record Record) error {
	if _, ok := im.storage[record.Key]; !ok {
		return fmt.Errorf("%s key does not exist", record.Key)
	}
	im.storage[record.Key] = record.Value
	return nil
}

func (im *InMemory) Delete(key string) error {
	delete(im.storage, key)
	return nil
}

func (im *InMemory) Read(key string) ([]byte, error) {
	if _, ok := im.storage[key]; !ok {
		return nil, fmt.Errorf("%s key does not exist", key)
	}
	return im.storage[key], nil
}

func (im *InMemory) Commit() error {
	path := filepath.Join(".", "data.log")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Println("defer error")
		}
	}()

	for key, value := range im.storage {
		log := key + ":" + string(value) + "\n"
		file.WriteString(log)
	}
	return nil
}

func (im *InMemory) Abort() error {
	path := filepath.Join(".", "data.log")
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Error when opening file: %s", err)
		return err
	}
	defer func() error {
		err = file.Close()
		return err
	}()

	for k := range im.storage {
		delete(im.storage, k)
	}

	fileScanner := bufio.NewScanner(file)

	for fileScanner.Scan() {
		data := strings.Split(fileScanner.Text(), ":")
		im.storage[data[0]] = []byte(data[1])
	}
	if err := fileScanner.Err(); err != nil {
		log.Fatalf("Error while reading file: %s", err)
		return err
	}
	return nil
}
