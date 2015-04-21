package main

import (
	"fmt"
	"github.com/wangjild/myreplication"
)

var (
	host     = "10.4.16.17"
	port     = 3307
	username = "repl"
	password = "replmtc50"
)

func main() {
	newConnection := myreplication.NewConnection()
	serverId := uint32(16173307)
	err := newConnection.ConnectAndAuth(host, port, username, password)

	if err != nil {
		panic("Client not connected and not autentificate to master server with error:" + err.Error())
	}
	//Get position and file name
	/*
		pos, filename, err := newConnection.GetMasterStatus()

		if err != nil {
			panic("Master status fail: " + err.Error())
		}*/

	//	pos, filename := 2549710, "mysql-bin.155900"
	pos, filename := 14904245, "mysql-bin.160668"
	el, err := newConnection.StartBinlogDump(uint32(pos), filename, serverId)

	if err != nil {
		panic("Cant start bin log: " + err.Error())
	}

	for {
		event, err := el.GetEvent()
		if err != nil {
			panic(err.Error())
		}

		switch e := event.(type) {
		case *myreplication.QueryEvent:
			//Output query event
			if e.GetQuery() != "BEGIN" {
				println("Query: " + e.GetQuery())
			}
		case *myreplication.IntVarEvent:
			//Output last insert_id  if statement based replication
			println(e.GetValue())
		case *myreplication.WriteEvent:
			//Output Write (insert) event
			// println("Write", e.GetTable())
			//Rows loop
			/*
				for i, row := range e.GetRows() {
					//Columns loop
					for j, col := range row {
						//Output row number, column number, column type and column value
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}*/
		case *myreplication.DeleteEvent:
			//Output delete event
			println("Delete", e.GetTable())
			for i, row := range e.GetRows() {
				for j, col := range row {
					println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
				}
			}
		case *myreplication.UpdateEvent:
			//Output update event
			// println("Update", e.GetTable())
			//Output old data before update
			/*
				for i, row := range e.GetRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
				//Output new
				for i, row := range e.GetNewRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}*/
		case *myreplication.ExecuteLoadQueryEvent:
			println("ExecuteLoad", e.GetQuery())
		default:
		}
	}

}
