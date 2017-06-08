/// Types that adopt this protocol qualify the JoinedPair type, which in turns
/// knows which kind of SQL join should be generated when it is fetched.
public protocol JoinKind { }
public enum LeftJoinKind : JoinKind { }
public enum InnerJoinKind : JoinKind { }

/// The scopes used to consume left and right parts of a joined pair.
enum JoinedPairScope : String {
    case left
    case right
}

/// The definition of the results of a joined query: (Left, Right) for inner
/// joins, and (Left, Right?) for left joins.
public struct JoinedPair<Left, Right, Join: JoinKind> { }


// MARK: - Inner Joinds

extension JoinedPair where Left: RowConvertible, Right: RowConvertible, Join == InnerJoinKind {
    
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

extension JoinedPair where Left: RowConvertible, Right: RowConvertible, Join == InnerJoinKind {
    
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


// MARK: - Left Joins

extension JoinedPair where Left: RowConvertible, Right: RowConvertible, Join == LeftJoinKind {
    
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

extension JoinedPair where Left: RowConvertible, Right: RowConvertible, Join == LeftJoinKind {
    
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
