
extension HasManyAssociation {
    public struct JoinedRequest {
        var leftRequest: QueryInterfaceRequest<Left>
        let association: HasManyAssociation<Left, Right>
    }
}

extension QueryInterfaceRequest where Fetched: TableMapping {
    public func joined<Right>(with association: HasManyAssociation<Fetched, Right>) -> HasManyAssociation<Fetched, Right>.JoinedRequest where Right: TableMapping {
        return HasManyAssociation.JoinedRequest(leftRequest: self, association: association)
    }
}

extension TableMapping {
    public static func joined<Right>(with association: HasManyAssociation<Self, Right>) -> HasManyAssociation<Self, Right>.JoinedRequest where Right: TableMapping {
        return all().joined(with: association)
    }
}

extension HasManyAssociation.JoinedRequest {
    private func updatingLeftRequest(_ transform: (QueryInterfaceRequest<Left>) -> (QueryInterfaceRequest<Left>)) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return HasManyAssociation.JoinedRequest(leftRequest: transform(leftRequest), association: association)
    }
    
    public func select(_ selection: SQLSelectable...) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(_ selection: [SQLSelectable]) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.select(sql: sql, arguments: arguments) }
    }
    
    public func distinct() -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.distinct() }
    }
    
    public func filter(_ predicate: SQLExpressible) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.filter(predicate) }
    }
    
    public func filter(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.filter(sql: sql, arguments: arguments) }
    }
    
    public func group(_ expressions: SQLExpressible...) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(_ expressions: [SQLExpressible]) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.group(sql: sql, arguments: arguments) }
    }
    
    public func having(_ predicate: SQLExpressible) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.having(predicate) }
    }
    
    public func having(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.having(sql: sql, arguments: arguments) }
    }
    
    public func order(_ orderings: SQLOrderingTerm...) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(_ orderings: [SQLOrderingTerm]) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.order(sql: sql, arguments: arguments) }
    }
    
    public func reversed() -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.reversed() }
    }
    
    public func limit(_ limit: Int, offset: Int? = nil) -> HasManyAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.limit(limit, offset: offset) }
    }
}

extension HasManyAssociation.JoinedRequest : TypedRequest {
    public typealias Fetched = (Left, Right)
    
    public func prepare(_ db: Database) throws -> (SelectStatement, RowAdapter?) {
        return try leftRequest.query
            .joined(.join, association.rightRequest.query, on: association.foreignKey(db))
            .prepare(db)
    }
}

// TODO: are those API useful?
extension HasManyAssociation.JoinedRequest where Left: RowConvertible, Right: RowConvertible {
    public func fetchCursor(_ db: Database) throws -> DatabaseCursor<(Left, Right)> {
        return try JoinedPair<Left, Right>.fetchCursor(db, self)
    }
    
    public func fetchAll(_ db: Database) throws -> [(Left, Right)] {
        return try JoinedPair<Left, Right>.fetchAll(db, self)
    }
    
    public func fetchOne(_ db: Database) throws -> (Left, Right)? {
        return try JoinedPair<Left, Right>.fetchOne(db, self)
    }
}

private struct JoinedPair<Left: RowConvertible, Right: RowConvertible> {
}

extension JoinedPair {
    
    // MARK: Fetching From SelectStatement
    
    static func fetchCursor(_ statement: SelectStatement, arguments: StatementArguments? = nil, adapter: RowAdapter? = nil) throws -> DatabaseCursor<(Left, Right)> {
        // Reuse a single mutable row for performance.
        let row = try Row(statement: statement).adapted(with: adapter, layout: statement)
        return statement.cursor(arguments: arguments, next: {
            let left = Left(row: row.scoped(on: "left")!)
            let right = Right(row: row.scoped(on: "right")!)
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
