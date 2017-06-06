public struct HasManyAssociation<Left, Right> {
    enum Mapping {
        case inferred
        case rightColumns([String])
    }
    let mapping: Mapping
    var rightRequest: QueryInterfaceRequest<Right>
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

extension QueryInterfaceRequest {
    public func including<Right>(_ association: HasManyAssociation<Fetched, Right>) -> GraphRequest<Fetched, Right> {
        return GraphRequest(leftRequest: self, association: association)
    }
}

extension TableMapping {
    public static func including<Right>(_ association: HasManyAssociation<Self, Right>) -> GraphRequest<Self, Right> {
        return all().including(association)
    }
}

public struct GraphRequest<Left, Right> {
    var leftRequest: QueryInterfaceRequest<Left>
    let association: HasManyAssociation<Left, Right>
}

extension GraphRequest {
    private func updatingLeftRequest(_ closure: (QueryInterfaceRequest<Left>) -> (QueryInterfaceRequest<Left>)) -> GraphRequest<Left, Right> {
        return GraphRequest(leftRequest: closure(leftRequest), association: association)
    }
    
    public func select(_ selection: SQLSelectable...) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(_ selection: [SQLSelectable]) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(sql: String, arguments: StatementArguments? = nil) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.select(sql: sql, arguments: arguments) }
    }
    
    public func distinct() -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.distinct() }
    }
    
    public func filter(_ predicate: SQLExpressible) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.filter(predicate) }
    }
    
    public func filter(sql: String, arguments: StatementArguments? = nil) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.filter(sql: sql, arguments: arguments) }
    }
    
    public func group(_ expressions: SQLExpressible...) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(_ expressions: [SQLExpressible]) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(sql: String, arguments: StatementArguments? = nil) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.group(sql: sql, arguments: arguments) }
    }
    
    public func having(_ predicate: SQLExpressible) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.having(predicate) }
    }
    
    public func having(sql: String, arguments: StatementArguments? = nil) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.having(sql: sql, arguments: arguments) }
    }
    
    public func order(_ orderings: SQLOrderingTerm...) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(_ orderings: [SQLOrderingTerm]) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(sql: String, arguments: StatementArguments? = nil) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.order(sql: sql, arguments: arguments) }
    }
    
    public func reversed() -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.reversed() }
    }
    
    public func limit(_ limit: Int, offset: Int? = nil) -> GraphRequest<Left, Right> {
        return updatingLeftRequest { $0.limit(limit, offset: offset) }
    }
}

extension GraphRequest where Left: RowConvertible & TableMapping, Right: RowConvertible & TableMapping {
    public func fetchAll(_ db: Database) throws -> [(Left, [Right])] {
        let mapping: [(from: String, to: String)]   // from: right column, to: left column
        switch association.mapping {
        case .inferred:
            let matchingForeignKeys = try db.foreignKeys(Right.databaseTableName)
                .filter { $0.tableName.lowercased() == Left.databaseTableName.lowercased() }
            switch matchingForeignKeys.count {
            case 0:
                fatalError("Table \(Right.databaseTableName) has no foreign key to table \(Left.databaseTableName)")
            case 1:
                mapping = matchingForeignKeys[0].mapping
            default:
                fatalError("Table \(Right.databaseTableName) has several foreign keys to table \(Left.databaseTableName)")
            }
        case .rightColumns(let rightColumns):
            let leftColumns: [String]
            if let primaryKey = try db.primaryKey(Left.databaseTableName) {
                leftColumns = primaryKey.columns
            } else {
                leftColumns = [Column.rowID.name]
            }
            guard leftColumns.count == rightColumns.count else {
                fatalError("Number of columns don't match")
            }
            mapping = zip(rightColumns, leftColumns).map { (from: $0, to: $1) }
        }
        
        var result: [(Left, [Right])] = []
        var leftKeys: [DatabaseValues] = []
        var resultIndexes : [DatabaseValues: Int] = [:]
        
        do {
            let leftCursor = try Row.fetchCursor(db, leftRequest)
            let leftStatement = leftCursor.statement
            let leftKeyIndexes = mapping.map { (_, leftColumn) -> Int in
                if let index = leftStatement.index(ofColumn: leftColumn) {
                    return index
                } else {
                    fatalError("Primary key of table \(Left.databaseTableName) is not selected")
                }
            }
            let enumeratedCursor = leftCursor.enumerated()
            while let (leftIndex, leftRow) = try enumeratedCursor.next() {
                let leftKey = DatabaseValues(dbValues: leftKeyIndexes.map { leftRow.value(atIndex: $0) })
                leftKeys.append(leftKey)
                resultIndexes[leftKey] = leftIndex
                result.append((Left(row: leftRow), []))
            }
        }
        
        if result.isEmpty {
            return result
        }
        
        do {
            // TODO: pick another technique when association.rightRequest has
            // a group/having/limit clause.
            let rightRequest: QueryInterfaceRequest<Right>
            if mapping.count == 1 {
                let leftKeyValues = leftKeys.lazy.map { $0.dbValues[0] }
                let rightColumn = mapping[0].from
                rightRequest = association.rightRequest.filter(leftKeyValues.contains(Column(rightColumn)))
            } else {
                fatalError("not implemented")
            }
            let rightCursor = try Row.fetchCursor(db, rightRequest)
            let rightStatement = rightCursor.statement
            let foreignKeyIndexes = mapping.map { (rightColumn, _) -> Int in
                if let index = rightStatement.index(ofColumn: rightColumn) {
                    return index
                } else {
                    fatalError("Foreign key of table \(Right.databaseTableName) is not selected")
                }
            }
            while let rightRow = try rightCursor.next() {
                let right = Right(row: rightRow)
                let foreignKey = DatabaseValues(dbValues: foreignKeyIndexes.map { rightRow.value(atIndex: $0) })
                let index = resultIndexes[foreignKey]!
                result[index].1.append(right)
            }
        }
        
        return result
    }
}

private struct DatabaseValues : Hashable {
    let dbValues : [DatabaseValue]
    
    var hashValue: Int {
        return dbValues.reduce(0) { $0 ^ $1.hashValue }
    }
    
    static func == (lhs: DatabaseValues, rhs: DatabaseValues) -> Bool {
        if lhs.dbValues.count != rhs.dbValues.count { return false }
        for (lhs, rhs) in zip(lhs.dbValues, rhs.dbValues) {
            if lhs != rhs { return false }
        }
        return true
    }
}
