public struct HasManyAssociation<Left: TableMapping, Right: TableMapping> {
    enum Mapping {
        case inferred
        case rightColumns([String])
    }
    let mapping: Mapping
    var rightRequest: QueryInterfaceRequest<Right>
    
    // from: right column, to: left column
    func foreignKeyMapping(_ db: Database) throws -> [(from: String, to: String)] {
        switch mapping {
        case .inferred:
            let matchingForeignKeys = try db.foreignKeys(Right.databaseTableName)
                .filter { $0.tableName.lowercased() == Left.databaseTableName.lowercased() }
            switch matchingForeignKeys.count {
            case 0:
                fatalError("Table \(Right.databaseTableName) has no foreign key to table \(Left.databaseTableName)")
            case 1:
                return matchingForeignKeys[0].mapping
            default:
                fatalError("Table \(Right.databaseTableName) has several foreign keys to table \(Left.databaseTableName)")
            }
        case .rightColumns(let rightColumns):
            // TODO: look for matching foreign key before defaulting to left primary key
            let leftColumns: [String]
            if let primaryKey = try db.primaryKey(Left.databaseTableName) {
                leftColumns = primaryKey.columns
            } else {
                leftColumns = [Column.rowID.name]
            }
            guard leftColumns.count == rightColumns.count else {
                fatalError("Number of columns don't match")
            }
            return zip(rightColumns, leftColumns).map { (from: $0, to: $1) }
        }
    }
}

extension HasManyAssociation {
    private func updatingRightRequest(_ closure: (QueryInterfaceRequest<Right>) -> QueryInterfaceRequest<Right>) -> HasManyAssociation<Left, Right> {
        return HasManyAssociation(mapping: mapping, rightRequest: closure(self.rightRequest))
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

extension TableMapping {
    public static func hasMany<Right>(_ right: Right.Type) -> HasManyAssociation<Self, Right> where Right: TableMapping {
        return HasManyAssociation(mapping: .inferred, rightRequest: Right.all())
    }
    
    public static func hasMany<Right>(_ right: Right.Type, from column: String) -> HasManyAssociation<Self, Right> where Right: TableMapping {
        return HasManyAssociation(mapping: .rightColumns([column]), rightRequest: Right.all())
    }
}
