package positionplugin

import (
	"errors"
	"github.com/siddontang/go-mysql/mysql"
	"strconv"
	"strings"
)

type Store interface {
	Save(mysql.Position) error
	Load() (mysql.Position, error)
}

func PositionToString(pos mysql.Position) string {
	return strconv.FormatUint(uint64(pos.Pos), 10) + " " + pos.Name
}

func PositionFromString(str string) (mysql.Position, error) {
	var pos mysql.Position
	if str == "" {
		return pos, errors.New("bad position")
	}
	arr := strings.SplitN(str, " ", 2)
	if len(arr) != 2 {
		return pos, errors.New("bad position")
	}
	i, err := strconv.ParseUint(arr[0], 10, 32)
	if err != nil {
		return pos, err
	}
	pos.Pos = uint32(i)
	pos.Name = arr[1]
	return pos, nil
}
