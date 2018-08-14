package config

import (
	"errors"
	"github.com/qjpcpu/binlog-canal/plugins/data-plugin"
	"github.com/qjpcpu/binlog-canal/plugins/position-plugin"
	"strings"
)

type TopicInfo struct {
	//table name
	Table string
	// default name is Schema name
	Topic string
	// default table name
	Key string
}

type Source struct {
	Schema string
	Tables []TopicInfo
}

type DBConfig struct {
	User   string
	Passwd string
	Net    string
	Addr   string
	DSN    string
}

type SourceConfig struct {
	//db conn: root:password@tcp(localhost:3306)
	MysqlConn string
	//parse from MysqlConn
	DBConfig struct {
		User    string
		Passwd  string
		Net     string
		Addr    string
		DSN     string
		Charset string
	} `json:"-"`
	//db and table list
	Sources []Source
	//if sources config is empty and SyncAll is true,broker sync all, else do nothing
	SyncAll bool
}

type Plugins struct {
	Position positionplugin.Store
	Data     dataplugin.Receiver
}

type ServerConfig struct {
	SourceConfig SourceConfig
}

func New(mysqlConn string, db_name string, tables ...string) ServerConfig {
	if len(tables) == 0 || db_name == "" || mysqlConn == "" {
		panic("params error")
	}
	source := Source{
		Schema: db_name,
	}
	for _, tbl := range tables {
		source.Tables = append(source.Tables, TopicInfo{
			Table: tbl,
		})
	}
	return ServerConfig{
		SourceConfig: SourceConfig{
			MysqlConn: mysqlConn,
			SyncAll:   false,
			Sources:   []Source{source},
		},
	}
}

func DefaultPlugins() Plugins {
	return PluginsOnlyForDebug()
}

func PluginsOnlyForDebug() Plugins {
	return Plugins{
		Position: positionplugin.MemStore{},
		Data:     dataplugin.Stdout{},
	}
}

func (cfg *SourceConfig) ParseDSN() error {
	dsn := cfg.MysqlConn
	var err error
	var left string = dsn
	for loop := true; loop; loop = false {
		var i int
		//user
		if i = strings.Index(left, ":"); i < 0 {
			err = errors.New("Invalid DSN: can not find user")
			break
		}
		cfg.DBConfig.User = left[0:i]
		i++
		left = left[i:]

		//password
		if i = strings.Index(left, "@"); i < 0 {
			err = errors.New("Invalid DSN: can not find passord")
			break
		}
		cfg.DBConfig.Passwd = left[0:i]
		i++
		left = left[i:]

		//addr
		if i = strings.Index(left, "("); i < 0 {
			err = errors.New("Invalid DSN: can not find addr")
			break
		}
		i++
		left = left[i:]

		if i = strings.Index(left, ")"); i < 0 {
			err = errors.New("Invalid DSN: can not find addr")
			break
		}
		cfg.DBConfig.Addr = left[0:i]

		left = left[i:]
		if i = strings.Index(left, "?"); i >= 0 {
			left = left[i+1:]
			pairs := strings.Split(left, "&")
			for _, pair := range pairs {
				kv := strings.Split(pair, "=")
				if len(kv) == 2 && kv[0] == "charset" {
					cfg.DBConfig.Charset = kv[1]
				}
			}
		}
	}
	return err
}
