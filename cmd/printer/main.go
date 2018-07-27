package main

// use siddontang go-mysql commit 535abe8f2eba8d1edaf1fecacea288ebb3b479ad

import (
	"fmt"
	"github.com/qjpcpu/binlog-canal/config"
	"github.com/qjpcpu/binlog-canal/sync"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	user := os.Getenv("DB_USER")
	pwd := os.Getenv("DB_PWD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	db_name := os.Getenv("DB_NAME")
	table := os.Getenv("DB_TABLE")
	if user == "" {
		user = "root"
	}
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "3306"
	}
	if db_name == "" || table == "" {
		fmt.Println("no DB_NAME or DB_TALBE found in env")
		os.Exit(1)
	}
	fmt.Printf("will print %s.%s data change\n\n", db_name, table)
	cfg := config.ServerConfig{
		SourceConfig: config.SourceConfig{
			MysqlConn: fmt.Sprintf("%s:%s@tcp(%s:%s)/?charset=utf8mb4&parseTime=true", user, pwd, host, port),
			SyncAll:   false,
			Sources: []config.Source{
				{
					Schema: db_name,
					Tables: []config.TopicInfo{
						{
							Table: table,
						},
					},
				},
			},
		},
	}
	plugins := config.PluginsOnlyForDebug()
	river, err := sync.NewSyncClient(&cfg, plugins)
	if err != nil {
		log.Fatalln(err)
	}
	if err := river.Start(); err != nil {
		log.Fatalln(err)
	}
	defer river.Close()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Interrupt, syscall.SIGTERM)
	<-sc
}
