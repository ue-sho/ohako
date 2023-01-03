package table

type Schema struct {
	length           uint32    // Fixed-length column size, i.e. the number of bytes used by one tuple
	columns          []*Column // All the columns in the schema, inlined and uninlined.
	tupleIsInlined   bool      // True if all the columns are inlined, false otherwise
	uninlinedColumns []uint32  // Indices of all uninlined columns
}

func NewSchema(columns []*Column) *Schema {
	schema := &Schema{}
	schema.tupleIsInlined = true

	var currentOffset uint32
	currentOffset = 0
	for i := uint32(0); i < uint32(len(columns)); i++ {
		column := columns[i]

		if !column.IsInlined() {
			schema.tupleIsInlined = false
			schema.uninlinedColumns = append(schema.uninlinedColumns, i)
		}

		column.columnOffset = currentOffset
		currentOffset += column.fixedLength

		schema.columns = append(schema.columns, column)
	}
	schema.length = currentOffset
	return schema
}

func (s *Schema) GetColumn(colIndex uint32) *Column {
	return s.columns[colIndex]
}

func (s *Schema) GetUnlinedColumns() []uint32 {
	return s.uninlinedColumns
}

func (s *Schema) GetColumnCount() uint32 {
	return uint32(len(s.columns))
}

func (s *Schema) Length() uint32 {
	return s.length
}

func (s *Schema) GetColIndex(columnName string) uint32 {
	for i := uint32(0); i < s.GetColumnCount(); i++ {
		if s.columns[i].columnName == columnName {
			return i
		}
	}

	panic("unreachable code") // this is not a good way to handle the issue
}

func (s *Schema) GetColumns() []*Column {
	return s.columns
}
