public struct HasOneAssociation<Left: TableMapping, Right: TableMapping> {
    let columnMappingRequest: ColumnMappingRequest
    var rightRequest: QueryInterfaceRequest<Right>
    
    func mapping(_ db: Database) throws -> [(left: String, right: String)] {
        let matchingColumnMappings = try columnMappingRequest.fetchAll(db)
        switch matchingColumnMappings.count {
        case 0:
            fatalError("Could not infer foreign key from \(Right.databaseTableName) to \(Left.databaseTableName)")
        case 1:
            return matchingColumnMappings[0].map { (left: $0.destination, right: $0.origin) }
        default:
            fatalError("Ambiguous foreign key from \(Right.databaseTableName) to \(Left.databaseTableName)")
        }
    }
}

extension HasOneAssociation {
    private func updatingRightRequest(_ transform: (QueryInterfaceRequest<Right>) -> QueryInterfaceRequest<Right>) -> HasOneAssociation<Left, Right> {
        return HasOneAssociation(columnMappingRequest: columnMappingRequest, rightRequest: transform(self.rightRequest))
    }
    
    public func select(_ selection: SQLSelectable...) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.select(selection) }
    }
    
    public func select(_ selection: [SQLSelectable]) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.select(selection) }
    }
    
    public func select(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.select(sql: sql, arguments: arguments) }
    }
    
    public func distinct() -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.distinct() }
    }
    
    public func filter(_ predicate: SQLExpressible) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.filter(predicate) }
    }
    
    public func filter(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.filter(sql: sql, arguments: arguments) }
    }
    
    public func group(_ expressions: SQLExpressible...) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.group(expressions) }
    }
    
    public func group(_ expressions: [SQLExpressible]) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.group(expressions) }
    }
    
    public func group(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.group(sql: sql, arguments: arguments) }
    }
    
    public func having(_ predicate: SQLExpressible) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.having(predicate) }
    }
    
    public func having(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.having(sql: sql, arguments: arguments) }
    }
    
    public func order(_ orderings: SQLOrderingTerm...) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.order(orderings) }
    }
    
    public func order(_ orderings: [SQLOrderingTerm]) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.order(orderings) }
    }
    
    public func order(sql: String, arguments: StatementArguments? = nil) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.order(sql: sql, arguments: arguments) }
    }
    
    public func reversed() -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.reversed() }
    }
    
    public func limit(_ limit: Int, offset: Int? = nil) -> HasOneAssociation<Left, Right> {
        return updatingRightRequest { $0.limit(limit, offset: offset) }
    }
}

extension HasOneAssociation where Left: MutablePersistable {
    func makeRequest(from record: Left) -> QueryInterfaceRequest<Right> {
        return rightRequest.filter { db in
            let mapping = try self.mapping(db)
            let container = PersistenceContainer(record)
            let rowValue = RowValue(mapping.map { container[caseInsensitive: $0.left]?.databaseValue ?? .null })
            return mapping.map { Column($0.right) } == rowValue
        }
    }
}