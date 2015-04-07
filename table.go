package myreplication

type Table struct {
	ColumnSchemas []string
	TableId       int32
	Schema        string
	Table         string
	Columns       []string
	PrimaryKey    []string
}
