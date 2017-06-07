// MARK: - SQLStar

struct SQLStar : SQLSelectable {
    let qualifier: SQLSourceQualifier?
    
    func resultColumnSQL(_ arguments: inout StatementArguments?) -> String {
        return "*"
    }
    
    func countedSQL(_ arguments: inout StatementArguments?) -> String {
        return "*"
    }
    
    func count(distinct: Bool) -> SQLCount? {
        // SELECT DISTINCT * FROM tableName ...
        guard !distinct else {
            return nil
        }
        
        // SELECT * FROM tableName ...
        // ->
        // SELECT COUNT(*) FROM tableName ...
        return .star
    }
    
    func numberOfColumns(_ db: Database) throws -> Int {
        guard let tableName = qualifier?.tableName else {
            fatalError("unqualified: can't count number of columns")
        }
        return try db.columnCount(in: tableName)
    }
    
    func qualified(by qualifier: SQLSourceQualifier) -> SQLStar {
        if self.qualifier == nil {
            return SQLStar(qualifier: qualifier)
        } else {
            return self
        }
    }
}


// MARK: - SQLAliasedExpression

struct SQLAliasedExpression : SQLSelectable {
    let expression: SQLExpression
    let alias: String
    
    init(_ expression: SQLExpression, alias: String) {
        self.expression = expression
        self.alias = alias
    }
    
    func resultColumnSQL(_ arguments: inout StatementArguments?) -> String {
        return expression.resultColumnSQL(&arguments) + " AS " + alias.quotedDatabaseIdentifier
    }
    
    func countedSQL(_ arguments: inout StatementArguments?) -> String {
        return expression.countedSQL(&arguments)
    }
    
    public func count(distinct: Bool) -> SQLCount? {
        return expression.count(distinct: distinct)
    }
    
    func qualified(by qualifier: SQLSourceQualifier) -> SQLAliasedExpression {
        return SQLAliasedExpression(expression.qualified(by: qualifier), alias: alias)
    }
    
    func numberOfColumns(_ db: Database) throws -> Int {
        return 1
    }
}
