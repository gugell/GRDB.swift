extension BelongsToAssociation {
    public struct LeftJoinedRequest {
        var leftRequest: QueryInterfaceRequest<Left>
        let association: BelongsToAssociation<Left, Right>
    }
}

extension QueryInterfaceRequest where Fetched: TableMapping {
    public func leftJoined<Right>(with association: BelongsToAssociation<Fetched, Right>) -> BelongsToAssociation<Fetched, Right>.LeftJoinedRequest where Right: TableMapping {
        return BelongsToAssociation.LeftJoinedRequest(leftRequest: self, association: association)
    }
}

extension TableMapping {
    public static func leftJoined<Right>(with association: BelongsToAssociation<Self, Right>) -> BelongsToAssociation<Self, Right>.LeftJoinedRequest where Right: TableMapping {
        return all().leftJoined(with: association)
    }
}

extension BelongsToAssociation.LeftJoinedRequest {
    private func updatingLeftRequest(_ transform: (QueryInterfaceRequest<Left>) -> (QueryInterfaceRequest<Left>)) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return BelongsToAssociation.LeftJoinedRequest(leftRequest: transform(leftRequest), association: association)
    }
    
    public func select(_ selection: SQLSelectable...) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(_ selection: [SQLSelectable]) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.select(sql: sql, arguments: arguments) }
    }
    
    public func distinct() -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.distinct() }
    }
    
    public func filter(_ predicate: SQLExpressible) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.filter(predicate) }
    }
    
    public func filter(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.filter(sql: sql, arguments: arguments) }
    }
    
    public func group(_ expressions: SQLExpressible...) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(_ expressions: [SQLExpressible]) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.group(sql: sql, arguments: arguments) }
    }
    
    public func having(_ predicate: SQLExpressible) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.having(predicate) }
    }
    
    public func having(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.having(sql: sql, arguments: arguments) }
    }
    
    public func order(_ orderings: SQLOrderingTerm...) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(_ orderings: [SQLOrderingTerm]) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.order(sql: sql, arguments: arguments) }
    }
    
    public func reversed() -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.reversed() }
    }
    
    public func limit(_ limit: Int, offset: Int? = nil) -> BelongsToAssociation<Left, Right>.LeftJoinedRequest {
        return updatingLeftRequest { $0.limit(limit, offset: offset) }
    }
}

extension BelongsToAssociation.LeftJoinedRequest : TypedRequest {
    public typealias Fetched = (Left, Right)
    
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

extension BelongsToAssociation.LeftJoinedRequest where Left: RowConvertible, Right: RowConvertible {
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
