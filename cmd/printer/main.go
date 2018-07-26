package main

import (
	"github.com/qjpcpu/binlog-canal/config"
	"github.com/qjpcpu/binlog-canal/sync"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := config.ServerConfig{
		SourceConfig: config.SourceConfig{
			MysqlConn: "USER:PASSWORD@tcp(127.0.0.1:3308)/?charset=utf8mb4&parseTime=true",
			SyncAll:   false,
			Sources: []config.Source{
				{
					Schema: "test_db",
					Tables: []config.TopicInfo{
						{
							Table: "test_table",
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
