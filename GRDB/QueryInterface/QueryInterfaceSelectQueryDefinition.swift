// MARK: - QueryInterfaceSelectQueryDefinition

struct QueryInterfaceSelectQueryDefinition {
    var selection: [SQLSelectable]
    var isDistinct: Bool
    var source: SQLSource?
    var whereExpression: ((Database) throws -> SQLExpression)?
    var groupByExpressions: [SQLExpression]
    var orderings: [SQLOrderingTerm]
    var isReversed: Bool
    var havingExpression: SQLExpression?
    var limit: SQLLimit?
    
    init(
        select selection: [SQLSelectable],
        isDistinct: Bool = false,
        from source: SQLSource? = nil,
        filter whereExpression: ((Database) throws -> SQLExpression)? = nil,
        groupBy groupByExpressions: [SQLExpression] = [],
        orderBy orderings: [SQLOrderingTerm] = [],
        isReversed: Bool = false,
        having havingExpression: SQLExpression? = nil,
        limit: SQLLimit? = nil)
    {
        self.selection = selection
        self.isDistinct = isDistinct
        self.source = source
        self.whereExpression = whereExpression
        self.groupByExpressions = groupByExpressions
        self.orderings = orderings
        self.isReversed = isReversed
        self.havingExpression = havingExpression
        self.limit = limit
    }
    
    func sql(_ db: Database, _ arguments: inout StatementArguments?) throws -> String {
        var sql = "SELECT"
        
        if isDistinct {
            sql += " DISTINCT"
        }
        
        assert(!selection.isEmpty)
        sql += " " + selection.map { $0.resultColumnSQL(&arguments) }.joined(separator: ", ")
        
        if let source = source {
            sql += try " FROM " + source.sourceSQL(db, &arguments)
        }
        
        if let whereExpression = try self.whereExpression?(db) {
            sql += " WHERE " + whereExpression.expressionSQL(&arguments)
        }
        
        if !groupByExpressions.isEmpty {
            sql += " GROUP BY " + groupByExpressions.map { $0.expressionSQL(&arguments) }.joined(separator: ", ")
        }
        
        if let havingExpression = havingExpression {
            sql += " HAVING " + havingExpression.expressionSQL(&arguments)
        }
        
        var orderings = self.orderings
        if isReversed {
            if orderings.isEmpty {
                // https://www.sqlite.org/lang_createtable.html#rowid
                //
                // > The rowid value can be accessed using one of the special
                // > case-independent names "rowid", "oid", or "_rowid_" in
                // > place of a column name. If a table contains a user defined
                // > column named "rowid", "oid" or "_rowid_", then that name
                // > always refers the explicitly declared column and cannot be
                // > used to retrieve the integer rowid value.
                //
                // Here we assume that rowid is not a custom column.
                // TODO: support for user-defined rowid column.
                // TODO: support for WITHOUT ROWID tables.
                orderings = [Column.rowID.desc]
            } else {
                orderings = orderings.map { $0.reversed }
            }
        }
        if !orderings.isEmpty {
            sql += " ORDER BY " + orderings.map { $0.orderingTermSQL(&arguments) }.joined(separator: ", ")
        }
        
        if let limit = limit {
            sql += " LIMIT " + limit.sql
        }
        
        return sql
    }
    
    func numberOfColumns(_ db: Database) throws -> Int {
        return try selection.reduce(0) { try $0 + $1.numberOfColumns(db) }
    }
    
    func qualified(by qualifier: SQLSourceQualifier) -> QueryInterfaceSelectQueryDefinition {
        let qualifiedSource: SQLSource?
        if let source = source {
            qualifiedSource = source.qualified(by: qualifier)
        } else {
            qualifiedSource = nil
        }
        
        let appliedQualifier = qualifiedSource?.qualifier! ?? qualifier
        let qualifiedSelection = selection.map { $0.qualified(by: appliedQualifier) }
        let qualifiedFilter = whereExpression.map { closure in
            { db in try closure(db).qualified(by: appliedQualifier) }
        }
        let qualifiedGroupByExpressions = groupByExpressions.map { $0.qualified(by: appliedQualifier) }
        let qualifiedOrderings = orderings.map { $0.qualified(by: appliedQualifier) }
        let qualifiedHavingExpression = havingExpression?.qualified(by: appliedQualifier)
        
        return QueryInterfaceSelectQueryDefinition(
            select: qualifiedSelection,
            isDistinct: isDistinct,
            from: qualifiedSource,
            filter: qualifiedFilter,
            groupBy: qualifiedGroupByExpressions,
            orderBy: qualifiedOrderings,
            isReversed: isReversed,
            having: qualifiedHavingExpression,
            limit: limit)
    }
    
    func joined(with rightQuery: QueryInterfaceSelectQueryDefinition, on foreignKey: ForeignKeyInfo) -> QueryInterfaceSelectQueryDefinition {
        // SELECT * FROM left ... -> SELECT left.* FROM left ...
        let leftQuery = self.qualified(by: SQLSourceQualifier(alias: "left")) // we'll be smarter later

        // SELECT * FROM right ... -> SELECT right.* FROM right ...
        let rightQuery = rightQuery.qualified(by: SQLSourceQualifier(alias: "right")) // we'll be smarter later
        
        // Gather selections
        let joinedSelection = leftQuery.selection + rightQuery.selection
        
        // Join sources
        guard let leftSource = leftQuery.source else { fatalError("Join requires a left source") }
        guard let rightSource = rightQuery.source else { fatalError("Join requires a right source") }
        let joinedSource = SQLSource.joined(left: leftSource, right: rightSource, foreignKey: foreignKey)
        
        // Result
        var joinedQuery = leftQuery
        joinedQuery.selection = joinedSelection
        joinedQuery.source = joinedSource
        return joinedQuery
    }
    
    func makeDeleteStatement(_ db: Database) throws -> UpdateStatement {
        guard groupByExpressions.isEmpty else {
            // Programmer error
            fatalError("Can't delete query with GROUP BY expression")
        }
        
        guard havingExpression == nil else {
            // Programmer error
            fatalError("Can't delete query with GROUP BY expression")
        }
        
        guard limit == nil else {
            // Programmer error
            fatalError("Can't delete query with limit")
        }
        
        var sql = "DELETE"
        var arguments: StatementArguments? = StatementArguments()
        
        if let source = source {
            sql += try " FROM " + source.sourceSQL(db, &arguments)
        }
        
        if let whereExpression = try self.whereExpression?(db) {
            sql += " WHERE " + whereExpression.expressionSQL(&arguments)
        }
        
        let statement = try db.makeUpdateStatement(sql)
        statement.arguments = arguments!
        return statement
    }
}

indirect enum SQLSource {
    case table(name: String, qualifier: SQLSourceQualifier?)
    case query(query: QueryInterfaceSelectQueryDefinition, qualifier: SQLSourceQualifier?)
    case joined(left: SQLSource, right: SQLSource, foreignKey: ForeignKeyInfo)
    
    var qualifier: SQLSourceQualifier? {
        switch self {
        case .table(_, let qualifier): return qualifier
        case .query(_, let qualifier): return qualifier
        case .joined(let leftSource, _, _): return leftSource.qualifier
        }
    }
    
    func sourceSQL(_ db: Database, _ arguments: inout StatementArguments?) throws -> String {
        switch self {
        case .table(let table, let qualifier):
            if let alias = qualifier?.alias {
                return table.quotedDatabaseIdentifier + " AS " + alias.quotedDatabaseIdentifier
            } else {
                return table.quotedDatabaseIdentifier
            }
        case .query(let query, let qualifier):
            if let alias = qualifier?.alias {
                return try "(" + query.sql(db, &arguments) + ") AS " + alias.quotedDatabaseIdentifier
            } else {
                return try "(" + query.sql(db, &arguments) + ")"
            }
        case .joined(let leftSource, let rightSource, let foreignKey):
            // left JOIN right ON ...
            var sql = ""
            sql += try leftSource.sourceSQL(db, &arguments)
            sql += " JOIN "
            sql += try rightSource.sourceSQL(db, &arguments)
            if !foreignKey.columnMapping.isEmpty {
                sql += " ON "
                
                let leftQualifier = leftSource.qualifier!
                let rightQualifier = rightSource.qualifier!
                
                sql += foreignKey.columnMapping
                    .map { rightColumn, leftColumn in
                        let leftColumn = Column(leftColumn).qualified(by: leftQualifier)
                        let rightColumn = Column(rightColumn).qualified(by: rightQualifier)
                        return (rightColumn == leftColumn).sql }
                    .joined(separator: " AND ")
                
                return sql
            }
            fatalError("not implemented")
        }
    }
    
    func qualified(by qualifier: SQLSourceQualifier) -> SQLSource {
        switch self {
        case .table(let tableName, let oldQualifier):
            if oldQualifier == nil {
                return .table(name: tableName, qualifier: SQLSourceQualifier(tableName: tableName, alias: qualifier.alias))
            } else {
                return self
            }
        case .query(let query, let oldQualifier):
            if oldQualifier == nil {
                return .query(query: query, qualifier: qualifier)
            } else {
                return self
            }
        case .joined(let leftSource, let rightSource, let foreignKey):
            return .joined(left: leftSource.qualified(by: qualifier), right: rightSource, foreignKey: foreignKey)
        }
    }
}

public class SQLSourceQualifier {
    let tableName: String?
    let alias: String?
    
    init(alias: String?) {
        self.tableName = nil
        self.alias = alias
    }
    
    init(tableName: String, alias: String?) {
        self.tableName = tableName
        self.alias = alias
    }
}

struct SQLLimit {
    let limit: Int
    let offset: Int?
    
    var sql: String {
        if let offset = offset {
            return "\(limit) OFFSET \(offset)"
        } else {
            return "\(limit)"
        }
    }
}

extension SQLCount {
    var sqlSelectable: SQLSelectable {
        switch self {
        case .star:
            return SQLExpressionCount(SQLStar(qualifier: nil))
        case .distinct(let expression):
            return SQLExpressionCountDistinct(expression)
        }
    }
}
