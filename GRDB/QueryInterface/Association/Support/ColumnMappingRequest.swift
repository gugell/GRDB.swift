/// ColumnMappingRequest gives associations the column mappings they need to
/// join tables.
///
/// Mappings come from foreign keys, when they exist in the database schema.
///
/// When the schema does not define any foreign key, we can still infer complete
/// mappings from partial information and primary keys.
struct ColumnMappingRequest {
    let originTable: String
    let destinationTable: String
    let originColumns: [String]?
    let destinationColumns: [String]?
    
    init(originTable: String, destinationTable: String, originColumns: [String]? = nil, destinationColumns: [String]? = nil) {
        self.originTable = originTable
        self.destinationTable = destinationTable
        self.originColumns = originColumns
        self.destinationColumns = destinationColumns
    }
    
    func fetchAll(_ db: Database) throws -> [[(origin: String, destination: String)]] {
        if let originColumns = originColumns, let destinationColumns = destinationColumns {
            // Total information: no need to query the database schema.
            GRDBPrecondition(originColumns.count == destinationColumns.count, "Number of columns don't match")
            return [zip(originColumns, destinationColumns).map {
                (origin: $0, destination: $1)
                }]
        }
        
        // Incomplete information: let's look for schema foreign keys
        let foreignKeys = try db.foreignKeys(originTable).filter { foreignKey in
            if destinationTable.lowercased() != foreignKey.destinationTable.lowercased() {
                return false
            }
            if let originColumns = originColumns {
                let originColumns = Set(originColumns.lazy.map { $0.lowercased() })
                let foreignKeyColumns = Set(foreignKey.mapping.lazy.map { $0.origin.lowercased() })
                if originColumns != foreignKeyColumns {
                    return false
                }
            }
            if let destinationColumns = destinationColumns {
                let destinationColumns = Set(destinationColumns.lazy.map { $0.lowercased() })
                let foreignKeyColumns = Set(foreignKey.mapping.lazy.map { $0.destination.lowercased() })
                if destinationColumns != foreignKeyColumns {
                    return false
                }
            }
            return true
        }
        
        guard foreignKeys.isEmpty else {
            return foreignKeys.map { $0.mapping }
        }
        
        // No matching foreign key found: use the destination primary key
        if let originColumns = originColumns {
            let destinationColumns: [String]
            if let primaryKey = try db.primaryKey(destinationTable) {
                destinationColumns = primaryKey.columns
            } else {
                destinationColumns = [Column.rowID.name]
            }
            if (originColumns.count == destinationColumns.count) {
                return [zip(originColumns, destinationColumns).map {
                    (origin: $0, destination: $1)
                    }]
            }
        }
        
        return []
    }
}
