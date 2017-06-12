public struct HasManyAssociation<Left: TableMapping, Right: TableMapping> {
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

extension HasManyAssociation {
    private func updatingRightRequest(_ transform: (QueryInterfaceRequest<Right>) -> QueryInterfaceRequest<Right>) -> HasManyAssociation<Left, Right> {
        return HasManyAssociation(columnMappingRequest: columnMappingRequest, rightRequest: transform(self.rightRequest))
    }
    
    public func select(_ selection: SQLSelectable...) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.select(selection) }
    }
    
    public func select(_ selection: [SQLSelectable]) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.select(selection) }
    }
    
    public func select(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.select(sql: sql, arguments: arguments) }
    }
    
    public func distinct() -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.distinct() }
    }
    
    public func filter(_ predicate: SQLExpressible) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.filter(predicate) }
    }
    
    public func filter(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.filter(sql: sql, arguments: arguments) }
    }
    
    public func group(_ expressions: SQLExpressible...) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.group(expressions) }
    }
    
    public func group(_ expressions: [SQLExpressible]) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.group(expressions) }
    }
    
    public func group(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.group(sql: sql, arguments: arguments) }
    }
    
    public func having(_ predicate: SQLExpressible) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.having(predicate) }
    }
    
    public func having(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.having(sql: sql, arguments: arguments) }
    }
    
    public func order(_ orderings: SQLOrderingTerm...) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.order(orderings) }
    }
    
    public func order(_ orderings: [SQLOrderingTerm]) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.order(orderings) }
    }
    
    public func order(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.order(sql: sql, arguments: arguments) }
    }
    
    public func reversed() -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.reversed() }
    }
    
    public func limit(_ limit: Int, offset: Int? = nil) -> HasManyAssociation<Left, Right> {
        return updatingRightRequest { $0.limit(limit, offset: offset) }
    }
}

extension HasManyAssociation where Left: MutablePersistable {
    func makeRequest(from record: Left) -> QueryInterfaceRequest<Right> {
        return rightRequest.filter { db in
            let mapping = try self.mapping(db)
            let container = PersistenceContainer(record)
            let rowValue = RowValue(mapping.map { container[caseInsensitive: $0.left]?.databaseValue ?? .null })
            return mapping.map { Column($0.right) } == rowValue
        }
    }
}
