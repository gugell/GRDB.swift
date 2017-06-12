extension TableMapping {
    public static func hasOne<Right>(_ right: Right.Type) -> HasOneAssociation<Self, Right> where Right: TableMapping {
        let columnMappingRequest = ColumnMappingRequest(
            originTable: Right.databaseTableName,
            destinationTable: databaseTableName)
        return HasOneAssociation(columnMappingRequest: columnMappingRequest, rightRequest: Right.all())
    }
    
    public static func hasOne<Right>(_ right: Right.Type, from column: String) -> HasOneAssociation<Self, Right> where Right: TableMapping {
        let columnMappingRequest = ColumnMappingRequest(
            originTable: Right.databaseTableName,
            destinationTable: databaseTableName,
            originColumns: [column])
        return HasOneAssociation(columnMappingRequest: columnMappingRequest, rightRequest: Right.all())
    }
    
    // TODO: multiple right columns
    // TODO: fully qualified foreign key (left + right columns)
}
