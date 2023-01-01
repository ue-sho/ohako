package storage

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type InMemory struct {
	storage map[string][]byte
}

func (im *InMemory) Insert(record Record) error {
	im.storage[record.Key] = record.Value
	return nil
}

func (im *InMemory) Update(record Record) error {
	im.storage[record.Key] = record.Value
	return nil
}

func (im *InMemory) Delete(key string) error {
	delete(im.storage, key)
	return nil
}

func (im *InMemory) Read(key string) ([]byte, error) {
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
		log := "{" + key + " : " + string(value) + "}\n"
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

	fileScanner := bufio.NewScanner(file)

	for fileScanner.Scan() {
		fmt.Println(fileScanner.Text())
	}
	if err := fileScanner.Err(); err != nil {
		log.Fatalf("Error while reading file: %s", err)
		return err
	}
	file.Close()
	return nil
}
