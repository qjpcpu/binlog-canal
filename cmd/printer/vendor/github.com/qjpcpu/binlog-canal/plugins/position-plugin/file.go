package positionplugin

import (
	"bufio"
	"bytes"
	"github.com/BurntSushi/toml"
	"github.com/siddontang/go-mysql/mysql"
	"io/ioutil"
)

type FileStore struct {
	File string
}

func NewFilePosition(file string) FileStore {
	return FileStore{File: file}
}

func (f FileStore) Save(pos mysql.Position) error {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encoder := toml.NewEncoder(writer)
	if err := encoder.Encode(pos); err != nil {
		return err
	}
	writer.Flush()
	return ioutil.WriteFile(f.File, buf.Bytes(), 0644)
}

func (f FileStore) Load() (mysql.Position, error) {
	pos := mysql.Position{}
	_, err := toml.DecodeFile(f.File, &pos)
	return pos, err
}
