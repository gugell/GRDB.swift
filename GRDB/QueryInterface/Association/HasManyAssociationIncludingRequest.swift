extension HasManyAssociation {
    public struct IncludingRequest {
        var leftRequest: QueryInterfaceRequest<Left>
        let association: HasManyAssociation<Left, Right>
    }
}

extension QueryInterfaceRequest where Fetched: TableMapping {
    public func including<Right>(_ association: HasManyAssociation<Fetched, Right>) -> HasManyAssociation<Fetched, Right>.IncludingRequest where Right: TableMapping {
        return HasManyAssociation.IncludingRequest(leftRequest: self, association: association)
    }
}

extension TableMapping {
    public static func including<Right>(_ association: HasManyAssociation<Self, Right>) -> HasManyAssociation<Self, Right>.IncludingRequest where Right: TableMapping {
        return all().including(association)
    }
}

extension HasManyAssociation.IncludingRequest {
    private func updatingLeftRequest(_ transform: (QueryInterfaceRequest<Left>) -> (QueryInterfaceRequest<Left>)) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return HasManyAssociation.IncludingRequest(leftRequest: transform(leftRequest), association: association)
    }
    
    public func select(_ selection: SQLSelectable...) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(_ selection: [SQLSelectable]) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.select(selection) }
    }
    
    public func select(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.select(sql: sql, arguments: arguments) }
    }
    
    public func distinct() -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.distinct() }
    }
    
    public func filter(_ predicate: SQLExpressible) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.filter(predicate) }
    }
    
    public func filter(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.filter(sql: sql, arguments: arguments) }
    }
    
    public func group(_ expressions: SQLExpressible...) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(_ expressions: [SQLExpressible]) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.group(expressions) }
    }
    
    public func group(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.group(sql: sql, arguments: arguments) }
    }
    
    public func having(_ predicate: SQLExpressible) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.having(predicate) }
    }
    
    public func having(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.having(sql: sql, arguments: arguments) }
    }
    
    public func order(_ orderings: SQLOrderingTerm...) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(_ orderings: [SQLOrderingTerm]) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.order(orderings) }
    }
    
    public func order(sql: String, arguments: StatementArguments? = nil) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.order(sql: sql, arguments: arguments) }
    }
    
    public func reversed() -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.reversed() }
    }
    
    public func limit(_ limit: Int, offset: Int? = nil) -> HasManyAssociation<Left, Right>.IncludingRequest {
        return updatingLeftRequest { $0.limit(limit, offset: offset) }
    }
}

extension HasManyAssociation.IncludingRequest where Left: RowConvertible, Right: RowConvertible {
    public func fetchAll(_ db: Database) throws -> [(Left, [Right])] {
        let foreignKey = try association.foreignKey(db)
        var result: [(Left, [Right])] = []
        var leftKeys: [RowValue] = []
        var resultIndexes : [RowValue: Int] = [:]
        
        // SELECT * FROM left...
        do {
            let cursor = try Row.fetchCursor(db, leftRequest)
            let foreignKeyIndexes = foreignKey.destinationColumns.map { leftColumn -> Int in
                if let index = cursor.statementIndex(ofColumn: leftColumn) {
                    return index
                } else {
                    fatalError("Column \(Left.databaseTableName).\(leftColumn) is not selected")
                }
            }
            let enumeratedCursor = cursor.enumerated()
            while let (recordIndex, row) = try enumeratedCursor.next() {
                let left = Left(row: row)
                let key = RowValue(foreignKeyIndexes.map { row.value(atIndex: $0) })
                leftKeys.append(key)
                resultIndexes[key] = recordIndex
                result.append((left, []))
            }
        }
        
        if result.isEmpty {
            return result
        }
        
        // SELECT * FROM right WHERE leftId IN (...)
        do {
            // TODO: pick another technique when association.rightRequest has
            // distinct/group/having/limit clause.
            //
            // TODO: Raw SQL snippets may be used to involve left and right columns at
            // the same time: consider joins.
            let rightRequest: QueryInterfaceRequest<Right>
            if foreignKey.columnMapping.count == 1 {
                let leftKeyValues = leftKeys.lazy.map { $0.dbValues[0] }
                let rightColumn = foreignKey.columnMapping[0].origin
                rightRequest = association.rightRequest.filter(leftKeyValues.contains(Column(rightColumn)))
            } else {
                fatalError("not implemented")
            }
            let cursor = try Row.fetchCursor(db, rightRequest)
            let foreignKeyIndexes = foreignKey.originColumns.map { rightColumn -> Int in
                if let index = cursor.statementIndex(ofColumn: rightColumn) {
                    return index
                } else {
                    fatalError("Column \(Right.databaseTableName).\(rightColumn) is not selected")
                }
            }
            while let row = try cursor.next() {
                let right = Right(row: row)
                let foreignKey = RowValue(foreignKeyIndexes.map { row.value(atIndex: $0) })
                let index = resultIndexes[foreignKey]!
                result[index].1.append(right)
            }
        }
        
        return result
    }
}
