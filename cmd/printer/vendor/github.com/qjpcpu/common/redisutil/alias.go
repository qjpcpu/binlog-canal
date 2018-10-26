package redisutil

import (
	"github.com/garyburd/redigo/redis"
)

type Conn = redis.Conn
type Script = redis.Script
type Pool = redis.Pool

var (
	ErrNil = redis.ErrNil
)

var (
	Int        = redis.Int
	Int64      = redis.Int64
	Uint64     = redis.Uint64
	Float64    = redis.Float64
	String     = redis.String
	Bytes      = redis.Bytes
	Bool       = redis.Bool
	Values     = redis.Values
	Float64s   = redis.Float64s
	Strings    = redis.Strings
	ByteSlices = redis.ByteSlices
	Int64s     = redis.Int64s
	Ints       = redis.Ints
	StringMap  = redis.StringMap
	IntMap     = redis.IntMap
	Int64Map   = redis.Int64Map
	Positions  = redis.Positions
	NewScript  = redis.NewScript
)
