package positionplugin

import (
	"errors"
	"github.com/siddontang/go-mysql/mysql"
)

type MemStore struct {
	pos mysql.Position
}

func (f MemStore) Save(pos mysql.Position) error {
	f.pos = pos
	return nil
}

func (f MemStore) Load() (mysql.Position, error) {
	if f.pos.Name == "" {
		return mysql.Position{}, errors.New("empty")
	} else {
		return f.pos, nil
	}
}
