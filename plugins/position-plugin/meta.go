package positionplugin

import (
	"github.com/siddontang/go-mysql/mysql"
)

type Store interface {
	Save(mysql.Position) error
	Load() (mysql.Position, error)
}
