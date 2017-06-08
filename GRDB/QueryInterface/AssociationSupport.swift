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
            GRDBPrecondition(originColumns.count == destinationColumns.count, "Number of columns don't match")
            return [zip(originColumns, destinationColumns).map {
                (origin: $0, destination: $1)
                }]
        }
        
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

enum JoinedPairScope : String {
    case left
    case right
}

struct JoinedPair<Left: RowConvertible, Right: RowConvertible> {
}

extension JoinedPair {
    
    // MARK: Fetching From SelectStatement
    
    static func fetchCursor(_ statement: SelectStatement, arguments: StatementArguments? = nil, adapter: RowAdapter? = nil) throws -> DatabaseCursor<(Left, Right)> {
        // Reuse a single mutable row for performance.
        let row = try Row(statement: statement).adapted(with: adapter, layout: statement)
        return statement.cursor(arguments: arguments, next: {
            let leftRow = row.scoped(on: JoinedPairScope.left)!
            let rightRow = row.scoped(on: JoinedPairScope.right)!
            
            let left = Left(row: leftRow)
            let right = Right(row: rightRow)
            
            return (left, right)
        })
    }
    
    static func fetchAll(_ statement: SelectStatement, arguments: StatementArguments? = nil, adapter: RowAdapter? = nil) throws -> [(Left, Right)] {
        return try Array(fetchCursor(statement, arguments: arguments, adapter: adapter))
    }
    
    static func fetchOne(_ statement: SelectStatement, arguments: StatementArguments? = nil, adapter: RowAdapter? = nil) throws -> (Left, Right)? {
        return try fetchCursor(statement, arguments: arguments, adapter: adapter).next()
    }
}

extension JoinedPair {
    
    // MARK: Fetching From Request
    
    static func fetchCursor(_ db: Database, _ request: Request) throws -> DatabaseCursor<(Left, Right)> {
        let (statement, adapter) = try request.prepare(db)
        return try fetchCursor(statement, adapter: adapter)
    }
    
    static func fetchAll(_ db: Database, _ request: Request) throws -> [(Left, Right)] {
        let (statement, adapter) = try request.prepare(db)
        return try fetchAll(statement, adapter: adapter)
    }
    
    static func fetchOne(_ db: Database, _ request: Request) throws -> (Left, Right)? {
        let (statement, adapter) = try request.prepare(db)
        return try fetchOne(statement, adapter: adapter)
    }
}

struct LeftJoinedPair<Left: RowConvertible, Right: RowConvertible> {
}

extension LeftJoinedPair {
    
    // MARK: Fetching From SelectStatement
    
    static func fetchCursor(_ statement: SelectStatement, arguments: StatementArguments? = nil, adapter: RowAdapter? = nil) throws -> DatabaseCursor<(Left, Right?)> {
        // Reuse a single mutable row for performance.
        let row = try Row(statement: statement).adapted(with: adapter, layout: statement)
        return statement.cursor(arguments: arguments, next: {
            let leftRow = row.scoped(on: JoinedPairScope.left)!
            let rightRow = row.scoped(on: JoinedPairScope.right)!
            
            let left = Left(row: leftRow)
            let right: Right? = rightRow.containsNonNullValues ? Right(row: rightRow) : nil
            
            return (left, right)
        })
    }
    
    static func fetchAll(_ statement: SelectStatement, arguments: StatementArguments? = nil, adapter: RowAdapter? = nil) throws -> [(Left, Right?)] {
        return try Array(fetchCursor(statement, arguments: arguments, adapter: adapter))
    }
    
    static func fetchOne(_ statement: SelectStatement, arguments: StatementArguments? = nil, adapter: RowAdapter? = nil) throws -> (Left, Right?)? {
        return try fetchCursor(statement, arguments: arguments, adapter: adapter).next()
    }
}

extension LeftJoinedPair {
    
    // MARK: Fetching From Request
    
    static func fetchCursor(_ db: Database, _ request: Request) throws -> DatabaseCursor<(Left, Right?)> {
        let (statement, adapter) = try request.prepare(db)
        return try fetchCursor(statement, adapter: adapter)
    }
    
    static func fetchAll(_ db: Database, _ request: Request) throws -> [(Left, Right?)] {
        let (statement, adapter) = try request.prepare(db)
        return try fetchAll(statement, adapter: adapter)
    }
    
    static func fetchOne(_ db: Database, _ request: Request) throws -> (Left, Right?)? {
        let (statement, adapter) = try request.prepare(db)
        return try fetchOne(statement, adapter: adapter)
    }
}
