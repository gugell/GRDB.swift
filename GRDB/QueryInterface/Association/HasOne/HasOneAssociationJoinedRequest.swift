extension HasOneAssociation {
    public struct JoinedRequest {
        var leftRequest: QueryInterfaceRequest<Left>
        let association: HasOneAssociation<Left, Right>
    }
}

extension QueryInterfaceRequest where Fetched: TableMapping {
    public func joined<Right>(with association: HasOneAssociation<Fetched, Right>) -> HasOneAssociation<Fetched, Right>.JoinedRequest where Right: TableMapping {
        return HasOneAssociation.JoinedRequest(leftRequest: self, association: association)
    }
}

extension TableMapping {
    public static func joined<Right>(with association: HasOneAssociation<Self, Right>) -> HasOneAssociation<Self, Right>.JoinedRequest where Right: TableMapping {
        return all().joined(with: association)
    }
}

extension HasOneAssociation.JoinedRequest {
    private func updatingLeftRequest(_ transform: (QueryInterfaceRequest<Left>) -> (QueryInterfaceRequest<Left>)) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return HasOneAssociation.JoinedRequest(leftRequest: transform(leftRequest), association: association)
    }
    
    public func select(_ selection: SQLSelectable...) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(_ selection: [SQLSelectable]) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.select(sql: sql, arguments: arguments) }
    }
    
    public func distinct() -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.distinct() }
    }
    
    public func filter(_ predicate: SQLExpressible) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.filter(predicate) }
    }
    
    public func filter(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.filter(sql: sql, arguments: arguments) }
    }
    
    public func group(_ expressions: SQLExpressible...) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(_ expressions: [SQLExpressible]) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.group(sql: sql, arguments: arguments) }
    }
    
    public func having(_ predicate: SQLExpressible) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.having(predicate) }
    }
    
    public func having(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.having(sql: sql, arguments: arguments) }
    }
    
    public func order(_ orderings: SQLOrderingTerm...) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(_ orderings: [SQLOrderingTerm]) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.order(sql: sql, arguments: arguments) }
    }
    
    public func reversed() -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.reversed() }
    }
    
    public func limit(_ limit: Int, offset: Int? = nil) -> HasOneAssociation<Left, Right>.JoinedRequest {
        return updatingLeftRequest { $0.limit(limit, offset: offset) }
    }
}

extension HasOneAssociation.JoinedRequest : TypedRequest {
    public typealias Fetched = JoinedPair<Left, Right, InnerJoinKind>
    
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

// TODO: write this as an extension of TypedRequest when Swift makes it possible.
extension HasOneAssociation.JoinedRequest where Left: RowConvertible, Right: RowConvertible {
    public func fetchCursor(_ db: Database) throws -> DatabaseCursor<(left: Left, right: Right)> {
        return try JoinedPair<Left, Right, InnerJoinKind>.fetchCursor(db, self)
    }
    
    public func fetchAll(_ db: Database) throws -> [(left: Left, right: Right)] {
        return try JoinedPair<Left, Right, InnerJoinKind>.fetchAll(db, self)
    }
    
    public func fetchOne(_ db: Database) throws -> (left: Left, right: Right)? {
        return try JoinedPair<Left, Right, InnerJoinKind>.fetchOne(db, self)
    }
}
