extension TableMapping {
    public static func belongsTo<Right>(_ right: Right.Type) -> BelongsToAssociation<Self, Right> where Right: TableMapping {
        let columnMappingRequest = ColumnMappingRequest(
            originTable: databaseTableName,
            destinationTable: Right.databaseTableName)
        return BelongsToAssociation(columnMappingRequest: columnMappingRequest, rightRequest: Right.all())
    }
    
    public static func belongsTo<Right>(_ right: Right.Type, from column: String) -> BelongsToAssociation<Self, Right> where Right: TableMapping {
        let columnMappingRequest = ColumnMappingRequest(
            originTable: databaseTableName,
            destinationTable: Right.databaseTableName,
            originColumns: [column])
        return BelongsToAssociation(columnMappingRequest: columnMappingRequest, rightRequest: Right.all())
    }
    
    // TODO: multiple right columns in columnMappingRequest
    // TODO: fully qualified foreign key (left + right columns) in columnMappingRequest
}
