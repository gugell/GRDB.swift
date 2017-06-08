extension BelongsToAssociation {
    public struct JoinedRequest {
        var leftRequest: QueryInterfaceRequest<Left>
        let association: BelongsToAssociation<Left, Right>
    }
}

extension QueryInterfaceRequest where Fetched: TableMapping {
    public func joined<Right>(with association: BelongsToAssociation<Fetched, Right>) -> BelongsToAssociation<Fetched, Right>.JoinedRequest where Right: TableMapping {
        return BelongsToAssociation.JoinedRequest(leftRequest: self, association: association)
    }
}

extension TableMapping {
    public static func joined<Right>(with association: BelongsToAssociation<Self, Right>) -> BelongsToAssociation<Self, Right>.JoinedRequest where Right: TableMapping {
        return all().joined(with: association)
    }
}

extension BelongsToAssociation.JoinedRequest {
    private func updatingLeftRequest(_ transform: (QueryInterfaceRequest<Left>) -> (QueryInterfaceRequest<Left>)) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return BelongsToAssociation.JoinedRequest(leftRequest: transform(leftRequest), association: association)
    }
    
    public func select(_ selection: SQLSelectable...) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(_ selection: [SQLSelectable]) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.select(sql: sql, arguments: arguments) }
    }
    
    public func distinct() -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.distinct() }
    }
    
    public func filter(_ predicate: SQLExpressible) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.filter(predicate) }
    }
    
    public func filter(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.filter(sql: sql, arguments: arguments) }
    }
    
    public func group(_ expressions: SQLExpressible...) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(_ expressions: [SQLExpressible]) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.group(sql: sql, arguments: arguments) }
    }
    
    public func having(_ predicate: SQLExpressible) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.having(predicate) }
    }
    
    public func having(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.having(sql: sql, arguments: arguments) }
    }
    
    public func order(_ orderings: SQLOrderingTerm...) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(_ orderings: [SQLOrderingTerm]) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.order(sql: sql, arguments: arguments) }
    }
    
    public func reversed() -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.reversed() }
    }
    
    public func limit(_ limit: Int, offset: Int? = nil) -> BelongsToAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.limit(limit, offset: offset) }
    }
}

extension BelongsToAssociation.JoinedRequest : TypedRequest {
    public typealias Fetched = (Left, Right)
    
    public func prepare(_ db: Database) throws -> (SelectStatement, RowAdapter?) {
        return try leftRequest.query
            .join(
                association.rightRequest.query,
                mapping: association.mapping(db),
                leftScope: JoinedPairScope.left.rawValue,
                rightScope: JoinedPairScope.right.rawValue)
            .prepare(db)
    }
}

extension BelongsToAssociation.JoinedRequest where Left: RowConvertible, Right: RowConvertible {
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
