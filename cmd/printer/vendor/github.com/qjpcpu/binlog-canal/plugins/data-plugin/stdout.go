package dataplugin

import (
	"fmt"
	"github.com/olekukonko/tablewriter"
	"github.com/qjpcpu/binlog-canal/protocol"
	"os"
	"strings"
)

type Stdout struct {
}

func (std Stdout) OnEventData(data *protocol.EventData) error {
	fmt.Fprintf(
		os.Stdout,
		"Action: %s    Schema: %s    Table:  %s\n",
		strings.ToUpper(string(data.Action)),
		data.Schema,
		data.Table,
	)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(data.Columns)
	for _, row := range data.Rows {
		line := make([]string, len(row))
		for i, cell := range row {
			line[i] = fmt.Sprint(cell)
		}
		table.Append(line)
	}
	table.Render()
	return nil
}
