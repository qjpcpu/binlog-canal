package sync

import (
	"github.com/qjpcpu/binlog-canal/protocol"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

type posSaver struct {
	pos   mysql.Position
	force bool
}

type eventHandler struct {
	c *SyncClient
}

// OnTableChanged is called when the table is created, altered, renamed or dropped.
// You need to clear the associated data like cache with the table.
// It will be called before OnDDL.
func (h *eventHandler) OnTableChanged(schema string, table string) error {
	// err := h.r.updateRule(schema, table)
	// if err != nil && err != ErrRuleNotExist {
	// 	return errors.Trace(err)
	// }
	return nil
}

// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
func (h *eventHandler) OnPosSynced(pos mysql.Position, force bool) error {
	return nil
}

func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	h.c.syncCh <- posSaver{pos: pos, force: true}

	return h.c.ctx.Err()
}

func (h *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.c.syncCh <- posSaver{nextPos, true}
	return h.c.ctx.Err()
}

func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	h.c.syncCh <- posSaver{nextPos, false}
	return h.c.ctx.Err()
}
func (h *eventHandler) OnGTID(mysql.GTIDSet) error {
	return nil
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	rule, ok := h.c.getFilterInfo(e.Table.Schema, e.Table.Name)
	if !ok {
		return nil
	}
	data := protocol.EventData{}
	data.Action = protocol.EventAction(e.Action)
	data.Schema = e.Table.Schema
	data.Table = e.Table.Name
	var hit_index bool
	for index, col := range e.Table.Columns {
		if !hit_index && rule.KeyIndex != defaultKeyIndex && col.Name == rule.Key {
			rule.KeyIndex = index
			hit_index = true
		}
		data.Columns = append(data.Columns, col.Name)
		if col.Type == schema.TYPE_STRING {
			for i, row := range e.Rows {
				if index < len(row) {
					if t, ok := e.Rows[i][index].([]byte); ok {
						e.Rows[i][index] = string(t)
					}
				}
			}
		}

	}
	//should not come here
	if rule.KeyIndex >= len(data.Columns) || rule.KeyIndex == defaultKeyIndex {
		rule.KeyIndex = 0
	}
	data.Owner = *rule
	data.Rows = e.Rows
	h.c.syncCh <- data
	return nil
}

func (h *eventHandler) String() string {
	return "BinlogEventHandler"
}
