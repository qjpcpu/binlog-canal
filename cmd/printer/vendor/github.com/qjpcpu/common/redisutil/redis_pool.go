package redisutil

import (
	"github.com/garyburd/redigo/redis"
	"gopkg.in/redsync.v1"
	"time"
)

var g_unblock_redis_pool *redis.Pool
var g_locker *redsync.Redsync

type Options struct {
	MaxIdle         int
	MaxActive       int
	IdleTimeout     time.Duration
	Wait            bool
	MaxConnLifetime time.Duration
	ConnTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

type OptFunc func(*Options)

func GetPool() *redis.Pool {
	return g_unblock_redis_pool
}

func DLocker() *redsync.Redsync {
	return g_locker
}

func CreatePool(conn string, redis_db, passwd string, wrappers ...OptFunc) *redis.Pool {
	var opt = &Options{
		MaxIdle:      200,
		MaxActive:    200,
		IdleTimeout:  2 * time.Second,
		Wait:         false,
		ConnTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	}
	for _, fn := range wrappers {
		fn(opt)
	}
	return &redis.Pool{
		MaxIdle:     opt.MaxIdle,
		MaxActive:   opt.MaxActive,
		IdleTimeout: opt.IdleTimeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout("tcp", conn, opt.ConnTimeout, opt.ReadTimeout, opt.WriteTimeout)
			if err != nil {
				return nil, err
			}

			if passwd != "" {
				if _, err := c.Do("AUTH", passwd); err != nil {
					c.Close()
					return nil, err
				}
			}

			if redis_db != "" {
				if _, err = c.Do("SELECT", redis_db); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func InitRedis(conn string, redis_db, passwd string, optfunc ...OptFunc) {
	g_unblock_redis_pool = CreatePool(conn, redis_db, passwd, optfunc...)
	g_locker = redsync.New([]redsync.Pool{g_unblock_redis_pool})
}
