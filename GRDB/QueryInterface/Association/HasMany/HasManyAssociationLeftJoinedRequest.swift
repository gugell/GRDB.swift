extension HasManyAssociation {
    public struct LeftJoinedRequest {
        var leftRequest: QueryInterfaceRequest<Left>
        let association: HasManyAssociation<Left, Right>
    }
}

extension QueryInterfaceRequest where Fetched: TableMapping {
    public func leftJoined<Right>(with association: HasManyAssociation<Fetched, Right>) -> HasManyAssociation<Fetched, Right>.LeftJoinedRequest where Right: TableMapping {
        return HasManyAssociation.LeftJoinedRequest(leftRequest: self, association: association)
    }
}

extension TableMapping {
    public static func leftJoined<Right>(with association: HasManyAssociation<Self, Right>) -> HasManyAssociation<Self, Right>.LeftJoinedRequest where Right: TableMapping {
        return all().leftJoined(with: association)
    }
}

extension HasManyAssociation.LeftJoinedRequest {
    private func updatingLeftRequest(_ transform: (QueryInterfaceRequest<Left>) -> (QueryInterfaceRequest<Left>)) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return HasManyAssociation.LeftJoinedRequest(leftRequest: transform(leftRequest), association: association)
    }
    
    public func select(_ selection: SQLSelectable...) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(_ selection: [SQLSelectable]) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.select(sql: sql, arguments: arguments) }
    }
    
    public func distinct() -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.distinct() }
    }
    
    public func filter(_ predicate: SQLExpressible) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.filter(predicate) }
    }
    
    public func filter(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.filter(sql: sql, arguments: arguments) }
    }
    
    public func group(_ expressions: SQLExpressible...) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(_ expressions: [SQLExpressible]) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.group(sql: sql, arguments: arguments) }
    }
    
    public func having(_ predicate: SQLExpressible) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.having(predicate) }
    }
    
    public func having(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.having(sql: sql, arguments: arguments) }
    }
    
    public func order(_ orderings: SQLOrderingTerm...) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(_ orderings: [SQLOrderingTerm]) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.order(sql: sql, arguments: arguments) }
    }
    
    public func reversed() -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.reversed() }
    }
    
    public func limit(_ limit: Int, offset: Int? = nil) -> HasManyAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.limit(limit, offset: offset) }
    }
}

extension HasManyAssociation.LeftJoinedRequest : TypedRequest {
    public typealias Fetched = JoinedPair<Left, Right, LeftJoinKind>
    
    public func prepare(_ db: Database) throws -> (SelectStatement, RowAdapter?) {
        return try leftRequest.query
            .leftJoin(
                association.rightRequest.query,
                mapping: association.mapping(db),
                leftScope: JoinedPairScope.left.rawValue,
                rightScope: JoinedPairScope.right.rawValue)
            .prepare(db)
    }
}

// TODO: write this as an extension of TypedRequest when Swift makes it possible.
extension HasManyAssociation.LeftJoinedRequest where Left: RowConvertible, Right: RowConvertible {
    public func fetchCursor(_ db: Database) throws -> DatabaseCursor<(Left, Right?)> {
        return try JoinedPair<Left, Right, LeftJoinKind>.fetchCursor(db, self)
    }
    
    public func fetchAll(_ db: Database) throws -> [(Left, Right?)] {
        return try JoinedPair<Left, Right, LeftJoinKind>.fetchAll(db, self)
    }
    
    public func fetchOne(_ db: Database) throws -> (Left, Right?)? {
        return try JoinedPair<Left, Right, LeftJoinKind>.fetchOne(db, self)
    }
}
