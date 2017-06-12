extension TableMapping {
    public static func hasMany<Right>(_ right: Right.Type) -> HasManyAssociation<Self, Right> where Right: TableMapping {
        let columnMappingRequest = ColumnMappingRequest(
            originTable: Right.databaseTableName,
            destinationTable: databaseTableName)
        return HasManyAssociation(columnMappingRequest: columnMappingRequest, rightRequest: Right.all())
    }
    
    public static func hasMany<Right>(_ right: Right.Type, from column: String) -> HasManyAssociation<Self, Right> where Right: TableMapping {
        let columnMappingRequest = ColumnMappingRequest(
            originTable: Right.databaseTableName,
            destinationTable: databaseTableName,
            originColumns: [column])
        return HasManyAssociation(columnMappingRequest: columnMappingRequest, rightRequest: Right.all())
    }
    
    // TODO: multiple right columns
    // TODO: fully qualified foreign key (left + right columns)
}

