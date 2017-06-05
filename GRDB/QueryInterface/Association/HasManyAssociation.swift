public struct HasManyAssociation<Left, Right> {
    let rightColumns: [String]
    var rightRequest: QueryInterfaceRequest<Right>
}

extension HasManyAssociation {
    private func updatingRightRequest(_ closure: (QueryInterfaceRequest<Right>) -> QueryInterfaceRequest<Right>) -> HasManyAssociation<Left, Right> {
        return HasManyAssociation(rightColumns: rightColumns, rightRequest: closure(self.rightRequest))
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
    public static func hasMany<Right>(_ right: Right.Type, from column: String) -> HasManyAssociation<Self, Right> where Right: TableMapping {
        return HasManyAssociation(rightColumns: [column], rightRequest: Right.all())
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

extension GraphRequest where Left: RowConvertible & TableMapping, Right: RowConvertible {
    public func fetchAll(_ db: Database) throws -> [(Left, [Right])] {
        if association.rightColumns.count == 1 {
            let rightColumn = association.rightColumns[0]
            var result: [(Left, [Right])] = []
            var leftPrimaryKeys: [DatabaseValue] = []
            var resultIndexes : [DatabaseValue: Int] = [:]
            
            let rowPrimaryKeyValue = try Left.rowPrimaryKeyValue(db)
            let leftCursor = try Row.fetchCursor(db, leftRequest).enumerated()
            while let (index, leftRow) = try leftCursor.next() {
                let leftPrimaryKey = rowPrimaryKeyValue(leftRow)
                leftPrimaryKeys.append(leftPrimaryKey)
                resultIndexes[leftPrimaryKey] = index
                result.append((Left(row: leftRow), []))
            }
            
            if result.isEmpty {
                return result
            }
            
            // TODO: pick another technique when association.rightRequest has
            // a group/having/limit clause.
            let rightRequest = association.rightRequest.filter(leftPrimaryKeys.contains(Column(rightColumn)))
            let rightCursor = try Row.fetchCursor(db, rightRequest)
            while let rightRow = try rightCursor.next() {
                let right = Right(row: rightRow)
                let leftPrimaryKey: DatabaseValue = rightRow.value(named: rightColumn)
                let index = resultIndexes[leftPrimaryKey]!
                result[index].1.append(right)
            }
            
            return result
        } else {
            fatalError("not implemented")
        }
    }
}
