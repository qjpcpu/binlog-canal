package sync

import (
	"github.com/qjpcpu/binlog-canal/plugins/position-plugin"
	"github.com/siddontang/go-mysql/mysql"
	"sync"
	"time"
)

func NewMetaInfo(storeImpl positionplugin.Store) (*MetaInfo, error) {
	if storeImpl == nil {
		panic("no position plugin installed")
	}
	m := &MetaInfo{}
	m.store = storeImpl
	pos, err := m.store.Load()
	if err != nil {
		return nil, err
	}
	m.Name = pos.Name
	m.Pos = pos.Pos
	return m, nil
}

type MetaInfo struct {
	sync.RWMutex `json:"-"`
	//binlog filename
	Name string
	//binlog position
	Pos uint32
	//save time
	LastSaveTime time.Time
	store        positionplugin.Store
}

func (p *MetaInfo) Save(pos mysql.Position) error {
	p.Lock()
	defer p.Unlock()
	return p.store.Save(pos)
}

func (p *MetaInfo) Position() mysql.Position {
	p.Lock()
	defer p.Unlock()
	return mysql.Position{Name: p.Name, Pos: p.Pos}
}

func (p *MetaInfo) Close() {
	pos := p.Position()
	p.Save(pos)
	return
}
