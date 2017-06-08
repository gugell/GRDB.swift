public struct BelongsToAssociation<Left: TableMapping, Right: TableMapping> {
    let columnMappingRequest: ColumnMappingRequest
    var rightRequest: QueryInterfaceRequest<Right>
    
    func mapping(_ db: Database) throws -> [(left: String, right: String)] {
        let matchingColumnMappings = try columnMappingRequest.fetchAll(db)
        switch matchingColumnMappings.count {
        case 0:
            fatalError("Could not infer foreign key from \(Left.databaseTableName) to \(Right.databaseTableName)")
        case 1:
            return matchingColumnMappings[0].map { (left: $0.origin, right: $0.destination) }
        default:
            fatalError("Ambiguous foreign key from \(Left.databaseTableName) to \(Right.databaseTableName)")
        }
    }
}

extension BelongsToAssociation {
    private func updatingRightRequest(_ transform: (QueryInterfaceRequest<Right>) -> QueryInterfaceRequest<Right>) -> BelongsToAssociation<Left, Right> {
        return BelongsToAssociation(columnMappingRequest: columnMappingRequest, rightRequest: transform(self.rightRequest))
    }
    
    public func select(_ selection: SQLSelectable...) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.select(selection) }
    }
    
    public func select(_ selection: [SQLSelectable]) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.select(selection) }
    }
    
    public func select(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.select(sql: sql, arguments: arguments) }
    }
    
    public func distinct() -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.distinct() }
    }
    
    public func filter(_ predicate: SQLExpressible) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.filter(predicate) }
    }
    
    public func filter(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.filter(sql: sql, arguments: arguments) }
    }
    
    public func group(_ expressions: SQLExpressible...) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.group(expressions) }
    }
    
    public func group(_ expressions: [SQLExpressible]) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.group(expressions) }
    }
    
    public func group(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.group(sql: sql, arguments: arguments) }
    }
    
    public func having(_ predicate: SQLExpressible) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.having(predicate) }
    }
    
    public func having(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.having(sql: sql, arguments: arguments) }
    }
    
    public func order(_ orderings: SQLOrderingTerm...) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.order(orderings) }
    }
    
    public func order(_ orderings: [SQLOrderingTerm]) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.order(orderings) }
    }
    
    public func order(sql: String, arguments: StatementArguments? = nil) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.order(sql: sql, arguments: arguments) }
    }
    
    public func reversed() -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.reversed() }
    }
    
    public func limit(_ limit: Int, offset: Int? = nil) -> BelongsToAssociation<Left, Right> {
        return updatingRightRequest { $0.limit(limit, offset: offset) }
    }
}

extension TableMapping {
    public static func belongsTo<Right>(_ right: Right.Type) -> BelongsToAssociation<Self, Right> where Right: TableMapping {
        let columnMappingRequest = ColumnMappingRequest(
            originTable: databaseTableName,
            destinationTable: Right.databaseTableName)
        return BelongsToAssociation(columnMappingRequest: columnMappingRequest, rightRequest: Right.all())
    }
    
    public static func belongsTo<Right>(_ right: Right.Type, from column: String) -> BelongsToAssociation<Self, Right> where Right: TableMapping {
        let columnMappingRequest = ColumnMappingRequest(
            originTable: databaseTableName,
            destinationTable: Right.databaseTableName,
            originColumns: [column])
        return BelongsToAssociation(columnMappingRequest: columnMappingRequest, rightRequest: Right.all())
    }
    
    // TODO: multiple right columns in columnMappingRequest
    // TODO: fully qualified foreign key (left + right columns) in columnMappingRequest
}

extension BelongsToAssociation where Left: MutablePersistable {
    public func owning(_ record: Left) -> QueryInterfaceRequest<Right> {
        return rightRequest.filter { db in
            let mapping = try self.mapping(db)
            let container = PersistenceContainer(record)
            let rowValue = RowValue(mapping.map { container[caseInsensitive: $0.left]?.databaseValue ?? .null })
            return mapping.map { Column($0.right) } == rowValue
        }
    }
}
