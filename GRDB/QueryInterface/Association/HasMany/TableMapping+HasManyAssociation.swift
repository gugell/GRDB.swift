extension TableMapping {
    public static func hasMany<Right>(_ right: Right.Type) -> HasManyAssociation<Self, Right> where Right: TableMapping {
        let columnMappingRequest = ColumnMappingRequest(
            originTable: Right.databaseTableName,
            destinationTable: databaseTableName)
        return HasManyAssociation(columnMappingRequest: columnMappingRequest, rightRequest: Right.all())
    }
    
    public static func hasMany<Right>(_ right: Right.Type, from originColumns: String...) -> HasManyAssociation<Self, Right> where Right: TableMapping {
        let columnMappingRequest = ColumnMappingRequest(
            originTable: Right.databaseTableName,
            destinationTable: databaseTableName,
            originColumns: originColumns)
        return HasManyAssociation(columnMappingRequest: columnMappingRequest, rightRequest: Right.all())
    }
    
    public static func hasMany<Right>(_ right: Right.Type, from originColumns: [String], to destinationColumns: [String]) -> HasManyAssociation<Self, Right> where Right: TableMapping {
        let columnMappingRequest = ColumnMappingRequest(
            originTable: Right.databaseTableName,
            destinationTable: databaseTableName,
            originColumns: originColumns,
            destinationColumns: destinationColumns)
        return HasManyAssociation(columnMappingRequest: columnMappingRequest, rightRequest: Right.all())
    }
}
