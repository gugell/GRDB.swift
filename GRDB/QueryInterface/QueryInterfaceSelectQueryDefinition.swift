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
    var adapter: ((Database) throws -> RowAdapter)?
    
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
        self.adapter = nil
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
        let qualifiedSelection = selection.map {
            $0.qualified(by: appliedQualifier)
        }
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
    
    // TODO: fix signature, it's ugly
    func joined(_ op: SQLJoinOperator, _ rightQuery: QueryInterfaceSelectQueryDefinition, on foreignKey: ForeignKeyInfo) -> QueryInterfaceSelectQueryDefinition {
        // SELECT * FROM left ... -> SELECT left.* FROM left ...
        let leftQuery = self.qualified(by: SQLSourceQualifier(alias: "left")) // we'll be smarter later

        // SELECT * FROM right ... -> SELECT right.* FROM right ...
        let rightQuery = rightQuery.qualified(by: SQLSourceQualifier(alias: "right")) // we'll be smarter later
        
        // Gather selections
        let joinedSelection = leftQuery.selection + rightQuery.selection
        
        // Join sources
        guard let leftSource = leftQuery.source else { fatalError("Join requires a left source") }
        guard let rightSource = rightQuery.source else { fatalError("Join requires a right source") }
        let joinedSource = SQLSource.joined(
            op: op,
            leftSource: leftSource,
            rightQuery: JoinedRightQuery(source: rightSource, onExpression: rightQuery.whereExpression),
            foreignKey: foreignKey)
        
        // TODO: take care of distinct
        // TODO: take care of order/isReversed
        // TODO: take care of group/having
        // TODO: take care of limit
        
        // Result
        var joinedQuery = leftQuery
        joinedQuery.selection = joinedSelection
        joinedQuery.source = joinedSource
        joinedQuery.adapter = { db in
            let leftCount = try leftQuery.numberOfColumns(db)
            let rightCount = try rightQuery.numberOfColumns(db)
            return ScopeAdapter([
                "left": RangeRowAdapter(0..<leftCount),
                "right": RangeRowAdapter(leftCount..<(leftCount + rightCount))])
        }
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

extension QueryInterfaceSelectQueryDefinition : Request {
    // Request protocol: customized count
    func fetchCount(_ db: Database) throws -> Int {
        return try Int.fetchOne(db, countQuery)!
    }
    
    func prepare(_ db: Database) throws -> (SelectStatement, RowAdapter?) {
        var arguments: StatementArguments? = StatementArguments()
        let sql = try self.sql(db, &arguments)
        let statement = try db.makeSelectStatement(sql)
        try statement.setArgumentsWithValidation(arguments!)
        return try (statement, adapter?(db))
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
    
    private var countQuery: QueryInterfaceSelectQueryDefinition {
        guard groupByExpressions.isEmpty && limit == nil else {
            // SELECT ... GROUP BY ...
            // SELECT ... LIMIT ...
            return trivialCountQuery
        }
        
        guard let source = source, case .table = source else {
            // SELECT ... FROM (something which is not a table)
            return trivialCountQuery
        }
        
        assert(!selection.isEmpty)
        if selection.count == 1 {
            guard let count = self.selection[0].count(distinct: isDistinct) else {
                return trivialCountQuery
            }
            var countQuery = unorderedQuery
            countQuery.isDistinct = false
            countQuery.selection = [count.sqlSelectable]
            return countQuery
        } else {
            // SELECT [DISTINCT] expr1, expr2, ... FROM tableName ...
            
            guard !isDistinct else {
                return trivialCountQuery
            }
            
            // SELECT expr1, expr2, ... FROM tableName ...
            // ->
            // SELECT COUNT(*) FROM tableName ...
            var countQuery = unorderedQuery
            countQuery.selection = [SQLExpressionCount(SQLStar(qualifier: nil))]
            return countQuery
        }
    }
    
    // SELECT COUNT(*) FROM (self)
    private var trivialCountQuery: QueryInterfaceSelectQueryDefinition {
        return QueryInterfaceSelectQueryDefinition(
            select: [SQLExpressionCount(SQLStar(qualifier: nil))],
            from: .query(query: unorderedQuery, qualifier: nil))
    }
    
    /// Remove ordering
    private var unorderedQuery: QueryInterfaceSelectQueryDefinition {
        var query = self
        query.isReversed = false
        query.orderings = []
        return query
    }
}

enum SQLJoinOperator : String {
    case join = "JOIN"
    case leftJoin = "LEFT JOIN"
}

struct JoinedRightQuery {
    let source: SQLSource
    let onExpression: ((Database) throws -> SQLExpression)?
}

indirect enum SQLSource {
    case table(name: String, qualifier: SQLSourceQualifier?)
    case query(query: QueryInterfaceSelectQueryDefinition, qualifier: SQLSourceQualifier?)
    case joined(op: SQLJoinOperator, leftSource: SQLSource, rightQuery: JoinedRightQuery, foreignKey: ForeignKeyInfo)
    
    var qualifier: SQLSourceQualifier? {
        switch self {
        case .table(_, let qualifier): return qualifier
        case .query(_, let qualifier): return qualifier
        case .joined(_, let leftSource, _, _): return leftSource.qualifier
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
        case .joined(let op, let leftSource, let rightQuery, let foreignKey):
            // left JOIN right ON ...
            var sql = ""
            sql += try leftSource.sourceSQL(db, &arguments)
            sql += " \(op.rawValue) "
            sql += try rightQuery.source.sourceSQL(db, &arguments)
            
            let leftQualifier = leftSource.qualifier!
            let rightQualifier = rightQuery.source.qualifier!
            
            var onClauses = foreignKey.columnMapping
                .map { (rightColumn, leftColumn) -> SQLExpression in
                    let leftColumn = Column(leftColumn).qualified(by: leftQualifier)
                    let rightColumn = Column(rightColumn).qualified(by: rightQualifier)
                    return (rightColumn == leftColumn) }
            if let onExpression = try rightQuery.onExpression?(db) {
                onClauses.append(onExpression)
            }
            if !onClauses.isEmpty {
                let onClause = onClauses.suffix(from: 1).reduce(onClauses.first!, &&)
                sql += " ON " + onClause.expressionSQL(&arguments)
            }
            
            return sql
        }
    }
    
    func qualified(by qualifier: SQLSourceQualifier) -> SQLSource {
        switch self {
        case .table(let tableName, let oldQualifier):
            if oldQualifier == nil {
                return .table(
                    name: tableName,
                    qualifier: SQLSourceQualifier(tableName: tableName, alias: qualifier.alias))
            } else {
                return self
            }
        case .query(let query, let oldQualifier):
            if oldQualifier == nil {
                return .query(query: query, qualifier: qualifier)
            } else {
                return self
            }
        case .joined(let op, let leftSource, let rightQuery, let foreignKey):
            return .joined(
                op: op,
                leftSource: leftSource.qualified(by: qualifier),
                rightQuery: rightQuery,
                foreignKey: foreignKey)
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
