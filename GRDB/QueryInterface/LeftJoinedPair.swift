enum LeftJoinedPairScope : String {
    case left
    case right
}

struct LeftJoinedPair<Left: RowConvertible, Right: RowConvertible> {
}

extension LeftJoinedPair {
    
    // MARK: Fetching From SelectStatement
    
    static func fetchCursor(_ statement: SelectStatement, arguments: StatementArguments? = nil, adapter: RowAdapter? = nil) throws -> DatabaseCursor<(Left, Right?)> {
        // Reuse a single mutable row for performance.
        let row = try Row(statement: statement).adapted(with: adapter, layout: statement)
        return statement.cursor(arguments: arguments, next: {
            let leftRow = row.scoped(on: LeftJoinedPairScope.left)!
            let rightRow = row.scoped(on: LeftJoinedPairScope.right)!
            
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
