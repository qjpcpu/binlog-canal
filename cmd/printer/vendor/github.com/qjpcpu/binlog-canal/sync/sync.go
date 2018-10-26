package sync

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/qjpcpu/binlog-canal/config"
	pg "github.com/qjpcpu/binlog-canal/plugins"
	"github.com/qjpcpu/binlog-canal/plugins/data-plugin"
	"github.com/qjpcpu/binlog-canal/plugins/position-plugin"
	"github.com/qjpcpu/binlog-canal/protocol"
	"github.com/qjpcpu/log"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"golang.org/x/net/context"
	"hash/fnv"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	defaultKey      = "TABLENAME"
	defaultKeyIndex = -1
)

var serverId uint32

type SyncClient struct {
	cfg    *config.ServerConfig
	canal  *canal.Canal
	rules  map[string]*protocol.TopicInfo
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	wgCfg  sync.WaitGroup
	master *MetaInfo
	syncCh chan interface{}
	sync.RWMutex
	sourceLock sync.RWMutex
	plugins    *pg.Plugins
}

func NewSyncClient(cfg *config.ServerConfig, plugins *pg.Plugins) (*SyncClient, error) {
	c := new(SyncClient)
	c.cfg = cfg
	c.rules = make(map[string]*protocol.TopicInfo)
	c.syncCh = make(chan interface{}, 4096)
	c.plugins = plugins
	if c.plugins.GetPositionStore() == nil {
		fmt.Fprintln(os.Stderr, "No position plugin installed, use in memory plugin")
		c.plugins.SetPositionStore(positionplugin.MemStore{})
	}
	if c.plugins.GetDataReceiver() == nil {
		fmt.Fprintln(os.Stderr, "No data plugin installed, use stdout")
		c.plugins.SetDataReceiver(dataplugin.Stdout{})
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())

	var err error
	if err = c.newCanal(); err != nil {
		return nil, err
	}

	if err = c.loadPositionInfo(); err != nil {
		return nil, err
	}

	if err = c.parseSourceRule(); err != nil {
		return nil, err
	}
	// We must use binlog full row image
	if err = c.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, err
	}

	return c, nil
}

func (r *SyncClient) Close() {
	log.Info("closing SyncClient")

	r.cancel()
	log.Info("closing SyncClient cancel ")

	r.canal.Close()
	log.Info("closing SyncClient canal.Close ")

	r.master.Close()
	log.Info("closing SyncClient master.Close ")
	log.Info("closing SyncClient wg.Wait Begin")
	r.wg.Wait()
	log.Info("closing SyncClient wg.Wait End ")

}

func (c *SyncClient) loadPositionInfo() error {
	sql := fmt.Sprintf(`SHOW MASTER STATUS;`)
	var err error
	for loop := true; loop; loop = false {
		c.master, err = NewMetaInfo(c.plugins.GetPositionStore())
		if err != nil {
			log.Infof("Get NewMetaInfo with err:%+v, refresh from mysql server", err)
			res := &mysql.Result{}
			if res, err = c.canal.Execute(sql); err != nil {
				break
			}
			pos := mysql.Position{}
			for i := 0; i < res.Resultset.RowNumber(); i++ {
				if pos.Name, err = res.GetString(i, 0); err != nil {
					break
				}
				var t int64
				if t, err = res.GetInt(i, 1); err != nil {
					break
				}
				pos.Pos = uint32(t)
				break
			}
			c.master = &MetaInfo{
				Name:  pos.Name,
				Pos:   pos.Pos,
				store: c.plugins.GetPositionStore(),
			}
			c.master.Save(pos)
		}
	}
	log.Infof("loadPositionInfo, name:%+v, pos:%+v, err:%+v ", c.master.Name, c.master.Pos, err)
	return err
}

//按照官方文档需要生产不同的server_id, 但是阿里的mysql即使生成相同的server_id也不会有问题
//这里以官方为准
func genMysqlSlaveServerId() uint32 {
	b := make([]byte, 32)
	rand.Read(b)
	s := base64.StdEncoding.EncodeToString(b)
	fnv_hash := fnv.New32()
	fnv_hash.Write([]byte(s))
	return fnv_hash.Sum32()
}

func (c *SyncClient) newCanal() error {
	cfg := canal.NewDefaultConfig()
	if err := c.cfg.SourceConfig.ParseDSN(); err != nil {
		return err
	}
	cfg.Addr = c.cfg.SourceConfig.DBConfig.Addr
	cfg.User = c.cfg.SourceConfig.DBConfig.User
	cfg.Password = c.cfg.SourceConfig.DBConfig.Passwd
	if c.cfg.SourceConfig.DBConfig.Charset != "" {
		cfg.Charset = c.cfg.SourceConfig.DBConfig.Charset
	}
	cfg.Flavor = "mysql"
	if serverId > 0 {
		cfg.ServerID = serverId
	} else {
		cfg.ServerID = genMysqlSlaveServerId()
		serverId = cfg.ServerID
	}
	cfg.Dump.DiscardErr = false
	cfg.Dump.ExecutionPath = ""

	var err error
	if c.canal, err = canal.NewCanal(cfg); err != nil {
		log.Infof("NewCanal err:%+v", err)
		return err
	}
	c.canal.SetEventHandler(&eventHandler{c})
	return err
}

func (c *SyncClient) Start() error {
	c.wg.Add(1)
	go c.syncLoop()

	pos := c.master.Position()
	if err := c.canal.RunFrom(pos); err != nil {
		log.Errorf("start canal err %v", err)
		return err
	}
	return nil
}

func (c *SyncClient) Ctx() context.Context {
	return c.ctx
}

func (c *SyncClient) syncLoop() {
	defer c.wg.Done()
	var pos mysql.Position
	tick := time.NewTicker(3 * time.Second)
	var newPos, needSavePos bool
	for {
		needSavePos = false
		select {
		case <-tick.C:
			needSavePos = newPos
		case v := <-c.syncCh:
			switch v := v.(type) {
			case posSaver:
				newPos = true
				pos = v.pos
				needSavePos = v.force
			case protocol.EventData:
				if err := c.triggerData(&v); err != nil {
					log.Errorf("trigger data finally fail:%v", err)
					return
				}
			default:
				log.Infof("get syncCh %+v", v)
			}
		case <-c.ctx.Done():
			return
		}
		if needSavePos {
			if err := c.master.Save(pos); err != nil {
				log.Errorf("save position to etcd err, pos:%+v, err:%+v, start retrySavePos", pos, err)
				if err := c.retrySavePos(pos); err != nil {
					log.Errorf("SyncClient retrySavePos err:%+v", err)
					return
				}

			}
			newPos = false
		}
	}
}

func (c *SyncClient) triggerData(evtData *protocol.EventData) error {
	term := time.Second * 5
	var max_retry = int64((1 * time.Hour) / term)
	var err error
	var cnt int64 = 1
	if err = c.plugins.GetDataReceiver().OnEventData(evtData); err == nil {
		return nil
	}
	for {
		if cnt >= max_retry {
			return fmt.Errorf("trigger event data fail:%s", err)
		}
		select {
		case <-time.After(term):
			cnt++
			if err = c.plugins.GetDataReceiver().OnEventData(evtData); err != nil {
				log.Errorf("consumer event data failed %v times:%v", cnt, err)
			} else {
				return nil
			}
		case <-c.ctx.Done():
			return fmt.Errorf("SyncClient Done")
		}
	}
}

func (c *SyncClient) retrySavePos(pos mysql.Position) error {
	var max_retry = int64((6 * time.Minute) / (5 * time.Second))
	tick := time.NewTicker(5 * time.Second)
	var cnt int64
	var err error
	for {
		if cnt >= max_retry {
			return fmt.Errorf("SyncClient runaway")
		}
		select {
		case <-tick.C:
			if err = c.master.Save(pos); err == nil {
				return nil
			} else {
				log.Errorf("save position to etcd err, pos:%+v, max_retry:%+v, cnt:%+v, err:%+v, try save after 5 seconds", pos, max_retry, cnt, err)
			}
			cnt++
		case <-c.ctx.Done():
			return fmt.Errorf("SyncClient Done")
		}
	}
}

func ruleKey(schema string, table string) string {
	return fmt.Sprintf("%s:%s", strings.ToLower(schema), strings.ToLower(table))
}

func (c *SyncClient) parseSourceRule() error {
	wildTables := make(map[string]bool)
	tmp_rule := make(map[string]*protocol.TopicInfo)
	var err error
OutLoop:
	for loop := true; loop; loop = false {
		if len(c.cfg.SourceConfig.Sources) == 0 && !c.cfg.SourceConfig.SyncAll {
			err = fmt.Errorf("THe Source config is empty, you may give a source configuration or set SyncAll=true")
			break OutLoop
		}
		for _, s := range c.cfg.SourceConfig.Sources {
			if len(s.Schema) == 0 {
				err = fmt.Errorf("empty schema not allowed for source")
				break OutLoop
			}
			if len(s.Tables) == 0 {
				tmp_rule[s.Schema] = &protocol.TopicInfo{Topic: strings.ToLower(s.Schema), Key: defaultKey, KeyIndex: defaultKeyIndex}
			}
			for _, table := range s.Tables {
				if len(table.Table) == 0 {
					err = fmt.Errorf("empty table not allowed for source")
					break OutLoop
				}
				//明确指定的配置才有效
				if regexp.QuoteMeta(table.Table) != table.Table {
					if _, ok := wildTables[ruleKey(s.Schema, table.Table)]; ok {
						err = fmt.Errorf("duplicate wildcard table defined for %s.%s", s.Schema, table.Table)
						break OutLoop
					}
					sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
	    table_name RLIKE "%s" AND table_schema = "%s";`, table.Table, s.Schema)

					res, err2 := c.canal.Execute(sql)
					if err2 != nil {
						err = err2
						break OutLoop
					}

					for i := 0; i < res.Resultset.RowNumber(); i++ {
						f, _ := res.GetString(i, 0)
						if r, err2 := c.genRule(&table, s.Schema, f); err2 == nil {
							tmp_rule[ruleKey(s.Schema, f)] = r
						} else {
							err = err2
							break OutLoop
						}
					}

					wildTables[ruleKey(s.Schema, table.Table)] = true
				} else {
					if r, err2 := c.genRule(&table, s.Schema, table.Table); err2 == nil {
						tmp_rule[ruleKey(s.Schema, table.Table)] = r
					} else {
						err = err2
						break OutLoop
					}
				}
			}
		}
	}
	if err == nil {
		c.sourceLock.Lock()
		defer c.sourceLock.Unlock()
		c.rules = tmp_rule
		for k, v := range c.rules {
			log.Infof("rule source:%+v,  topic:%+v", k, *v)
		}
	} else {
		log.Errorf("config err:%+v, source:%+v", err, c.cfg.SourceConfig.Sources)
	}
	log.Infof("config err:%+v, source:%+v", err, c.cfg.SourceConfig.Sources)
	return err
}

func (c *SyncClient) genRule(source *config.TopicInfo, schema, table string) (*protocol.TopicInfo, error) {
	var err error
	rule := &protocol.TopicInfo{KeyIndex: defaultKeyIndex}
	rule.Topic, rule.Key = strings.ToLower(source.Topic), strings.ToLower(source.Key)
	for loop := true; loop; loop = false {
		if source.Topic != "" && source.Key != "" {
			rule.Topic, rule.Key = source.Topic, strings.ToLower(source.Key)
			tmp, err2 := c.canal.GetTable(schema, table)
			if err2 != nil {
				err = err2
				break
			}
			for i, item := range tmp.Columns {
				if rule.Key == strings.ToLower(item.Name) {
					rule.KeyIndex = i
					break
				}
			}
			if rule.KeyIndex == defaultKeyIndex {
				err = fmt.Errorf("%+v.%+v, %s not exist", schema, table, source.Key)
			}
			break
		}
		if source.Topic == "" && source.Key == "" {
			rule.Topic, rule.Key = strings.ToLower(schema), strings.ToLower(table)
			break
		}
		err = fmt.Errorf("topic，key value,  should be null or not null at the same time, schema:%+v, table:%+v", schema, table)
	}
	return rule, err
}

func (c *SyncClient) getFilterInfo(schema, table string) (*protocol.TopicInfo, bool) {
	c.sourceLock.RLock()
	defer c.sourceLock.RUnlock()
	var ret *protocol.TopicInfo
	var is_ok bool
	for loop := true; loop; loop = false {
		if rule, ok := c.rules[ruleKey(schema, table)]; ok {
			ret, is_ok = rule, true
			break
		}
		if rule, ok := c.rules[schema]; ok {
			if rule.Key == defaultKey {
				rule.Key = strings.ToLower(table)
			}
			ret, is_ok = rule, true
			break
		}
		if len(c.rules) == 0 {
			ret, is_ok = &protocol.TopicInfo{Topic: strings.ToLower(schema), Key: strings.ToLower(table), KeyIndex: defaultKeyIndex}, true
			break
		}
	}
	return ret, is_ok
}
