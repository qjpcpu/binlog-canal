package positionplugin

import (
	"github.com/qjpcpu/common/redisutil"
	"github.com/siddontang/go-mysql/mysql"
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
	_, err := conn.Do("SET", f.key, PositionToString(pos))
	conn.Close()
	return err
}

func (f *RedisStore) Load() (mysql.Position, error) {
	conn := f.pool.Get()
	defer conn.Close()
	val, err := redisutil.String(conn.Do("GET", f.key))
	if err != nil {
		return mysql.Position{}, err
	}
	return PositionFromString(val)
}
