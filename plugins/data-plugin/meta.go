package dataplugin

import (
	"github.com/qjpcpu/binlog-canal/protocol"
)

type Receiver interface {
	OnEventData(*protocol.EventData) error
}
