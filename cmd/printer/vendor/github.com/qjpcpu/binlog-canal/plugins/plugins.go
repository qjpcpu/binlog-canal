package plugins

import (
	"github.com/qjpcpu/binlog-canal/plugins/data-plugin"
	"github.com/qjpcpu/binlog-canal/plugins/position-plugin"
)

type Plugins struct {
	position positionplugin.Store
	data     dataplugin.Receiver
}

func New() *Plugins {
	return &Plugins{
		position: positionplugin.MemStore{},
		data:     dataplugin.Stdout{},
	}
}

func (ps *Plugins) GetDataReceiver() dataplugin.Receiver {
	return ps.data
}

func (ps *Plugins) GetPositionStore() positionplugin.Store {
	return ps.position
}

func (ps *Plugins) SetDataReceiver(d dataplugin.Receiver) {
	ps.data = d
}

func (ps *Plugins) SetPositionStore(p positionplugin.Store) {
	ps.position = p
}
