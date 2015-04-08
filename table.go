package myreplication

type Table struct {
	SchemaColumns []*SchemaColumn
	TableId       int32
	Schema        string
	Table         string
	Columns       []string
	PrimaryKey    []string
}
