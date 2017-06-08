extension HasOneAssociation {
    public struct LeftJoinedRequest {
        var leftRequest: QueryInterfaceRequest<Left>
        let association: HasOneAssociation<Left, Right>
    }
}

extension QueryInterfaceRequest where Fetched: TableMapping {
    public func leftJoined<Right>(with association: HasOneAssociation<Fetched, Right>) -> HasOneAssociation<Fetched, Right>.LeftJoinedRequest where Right: TableMapping {
        return HasOneAssociation.LeftJoinedRequest(leftRequest: self, association: association)
    }
}

extension TableMapping {
    public static func leftJoined<Right>(with association: HasOneAssociation<Self, Right>) -> HasOneAssociation<Self, Right>.LeftJoinedRequest where Right: TableMapping {
        return all().leftJoined(with: association)
    }
}

extension HasOneAssociation.LeftJoinedRequest {
    private func updatingLeftRequest(_ transform: (QueryInterfaceRequest<Left>) -> (QueryInterfaceRequest<Left>)) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return HasOneAssociation.LeftJoinedRequest(leftRequest: transform(leftRequest), association: association)
    }
    
    public func select(_ selection: SQLSelectable...) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(_ selection: [SQLSelectable]) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.select(sql: sql, arguments: arguments) }
    }
    
    public func distinct() -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.distinct() }
    }
    
    public func filter(_ predicate: SQLExpressible) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.filter(predicate) }
    }
    
    public func filter(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.filter(sql: sql, arguments: arguments) }
    }
    
    public func group(_ expressions: SQLExpressible...) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(_ expressions: [SQLExpressible]) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.group(sql: sql, arguments: arguments) }
    }
    
    public func having(_ predicate: SQLExpressible) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.having(predicate) }
    }
    
    public func having(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.having(sql: sql, arguments: arguments) }
    }
    
    public func order(_ orderings: SQLOrderingTerm...) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(_ orderings: [SQLOrderingTerm]) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.order(sql: sql, arguments: arguments) }
    }
    
    public func reversed() -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.reversed() }
    }
    
    public func limit(_ limit: Int, offset: Int? = nil) -> HasOneAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.limit(limit, offset: offset) }
    }
}

extension HasOneAssociation.LeftJoinedRequest : TypedRequest {
    public typealias Fetched = (Left, Right)
    
    public func prepare(_ db: Database) throws -> (SelectStatement, RowAdapter?) {
        return try leftRequest.query
            .leftJoin(
                association.rightRequest.query,
                mapping: association.mapping(db),
                leftScope: LeftJoinedPairScope.left.rawValue,
                rightScope: LeftJoinedPairScope.right.rawValue)
            .prepare(db)
    }
}

extension HasOneAssociation.LeftJoinedRequest where Left: RowConvertible, Right: RowConvertible {
    public func fetchCursor(_ db: Database) throws -> DatabaseCursor<(Left, Right?)> {
        return try LeftJoinedPair<Left, Right>.fetchCursor(db, self)
    }
    
    public func fetchAll(_ db: Database) throws -> [(Left, Right?)] {
        return try LeftJoinedPair<Left, Right>.fetchAll(db, self)
    }
    
    public func fetchOne(_ db: Database) throws -> (Left, Right?)? {
        return try LeftJoinedPair<Left, Right>.fetchOne(db, self)
    }
}
