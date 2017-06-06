public struct HasManyAssociation<Left: TableMapping, Right: TableMapping> {
    enum ForeignKeyDefinition {
        case inferred
        case rightColumns([String])
        // TODO: fully qualified foreign key
    }
    
    let foreignKeyDefinition: ForeignKeyDefinition
    var rightRequest: QueryInterfaceRequest<Right>
    
    func foreignKey(_ db: Database) throws -> ForeignKeyInfo {
        switch foreignKeyDefinition {
        case .inferred:
            let matchingForeignKeys = try db.foreignKeys(Right.databaseTableName)
                .filter { $0.destinationTable.lowercased() == Left.databaseTableName.lowercased() }
            
            switch matchingForeignKeys.count {
            case 0:
                fatalError("Could not infer foreign key from \(Right.databaseTableName) to \(Left.databaseTableName)")
            case 1:
                return matchingForeignKeys[0]
            default:
                fatalError("Ambiguous foreign key from \(Right.databaseTableName) to \(Left.databaseTableName)")
            }
            
        case .rightColumns(let rightColumns):
            let rightColumnSet = Set(rightColumns.lazy.map { $0.lowercased() })
            let matchingForeignKeys = try db.foreignKeys(Right.databaseTableName)
                .filter { $0.destinationTable.lowercased() == Left.databaseTableName.lowercased() }
                .filter { Set($0.originColumns.lazy.map { $0.lowercased() }) == rightColumnSet }
            
            switch matchingForeignKeys.count {
            case 0:
                // Use primary key
                let leftColumns: [String]
                if let primaryKey = try db.primaryKey(Left.databaseTableName) {
                    leftColumns = primaryKey.columns
                } else {
                    leftColumns = [Column.rowID.name]
                }
                guard leftColumns.count == rightColumns.count else {
                    fatalError("Number of columns don't match")
                }
                let columnMapping = zip(rightColumns, leftColumns).map { (origin: $0, destination: $1) }
                return ForeignKeyInfo(destinationTable: Left.databaseTableName, columnMapping: columnMapping)
            case 1:
                return matchingForeignKeys[0]
            default:
                fatalError("Ambiguous foreign key from \(Right.databaseTableName) to \(Left.databaseTableName)")
            }
        }
    }
}

extension HasManyAssociation {
    private func updatingRightRequest(_ transform: (QueryInterfaceRequest<Right>) -> QueryInterfaceRequest<Right>) -> HasManyAssociation<Left, Right> {
        return HasManyAssociation(foreignKeyDefinition: foreignKeyDefinition, rightRequest: transform(self.rightRequest))
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
    func belonging(to record: Left) -> QueryInterfaceRequest<Right> {
        return rightRequest.filter { db in
            let foreignKey = try self.foreignKey(db)
            let container = PersistenceContainer(record)
            let rowValue = RowValue(foreignKey.destinationColumns.map { container[caseInsensitive: $0]?.databaseValue ?? .null })
            return foreignKey.destinationColumns == rowValue
        }
    }
}

extension TableMapping {
    public static func hasMany<Right>(_ right: Right.Type) -> HasManyAssociation<Self, Right> where Right: TableMapping {
        return HasManyAssociation(foreignKeyDefinition: .inferred, rightRequest: Right.all())
    }
    
    public static func hasMany<Right>(_ right: Right.Type, from column: String) -> HasManyAssociation<Self, Right> where Right: TableMapping {
        return HasManyAssociation(foreignKeyDefinition: .rightColumns([column]), rightRequest: Right.all())
    }
    
    // TODO: multiple right columns
    // TODO: fully qualified foreign key (left + right columns)
}
