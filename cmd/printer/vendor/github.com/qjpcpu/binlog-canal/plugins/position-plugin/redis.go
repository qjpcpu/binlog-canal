package positionplugin

import (
	"errors"
	"github.com/qjpcpu/common/redisutil"
	"github.com/siddontang/go-mysql/mysql"
	"strconv"
	"strings"
)

type RedisStore struct {
	pool *redisutil.Pool
	key  string
}

func NewRedisPosition(addr, db, pwd, key string) *RedisStore {
	return &RedisStore{pool: redisutil.CreatePool(addr, db, pwd), key: key}
}

func (f *RedisStore) Save(pos mysql.Position) error {
	conn := f.pool.Get()
	val := strconv.FormatUint(uint64(pos.Pos), 10) + " " + pos.Name
	_, err := conn.Do("SET", f.key, val)
	conn.Close()
	return err
}

func (f *RedisStore) Load() (mysql.Position, error) {
	conn := f.pool.Get()
	defer conn.Close()
	pos := mysql.Position{}
	val, err := redisutil.String(conn.Do("GET", f.key))
	if err != nil {
		return pos, err
	}
	if val == "" {
		return pos, errors.New("no positon found")
	}
	arr := strings.SplitN(val, " ", 2)
	i, err := strconv.ParseUint(arr[0], 10, 32)
	if err != nil {
		return pos, err
	}
	pos.Pos = uint32(i)
	pos.Name = arr[1]
	return pos, nil
}
