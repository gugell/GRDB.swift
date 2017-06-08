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
        limit: SQLLimit? = nil,
        adapter: ((Database) throws -> RowAdapter)? = nil)
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
        self.adapter = adapter
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
    
    func join(_ rightQuery: QueryInterfaceSelectQueryDefinition, mapping: [(left: String, right: String)], leftScope: String, rightScope: String) -> QueryInterfaceSelectQueryDefinition {
        return join(rightQuery, mapping: mapping, leftScope: leftScope, rightScope: rightScope, operator: .join)
    }
    
    func leftJoin(_ rightQuery: QueryInterfaceSelectQueryDefinition, mapping: [(left: String, right: String)], leftScope: String, rightScope: String) -> QueryInterfaceSelectQueryDefinition {
        return join(rightQuery, mapping: mapping, leftScope: leftScope, rightScope: rightScope, operator: .leftJoin)
    }
    
    private func join(_ rightQuery: QueryInterfaceSelectQueryDefinition, mapping: [(left: String, right: String)], leftScope: String, rightScope: String, operator joinOp: SQLJoinOperator) -> QueryInterfaceSelectQueryDefinition {
        // Left constraints
        GRDBPrecondition(groupByExpressions.isEmpty, "Can't join from query with GROUP BY expression")
        GRDBPrecondition(havingExpression == nil, "Can't join from query with GROUP BY expression")
        GRDBPrecondition(adapter == nil, "Support for left row adapter is not implemented")
        
        // Right constraints
        GRDBPrecondition(!rightQuery.isDistinct, "Can't join with distinct query")
        GRDBPrecondition(rightQuery.groupByExpressions.isEmpty, "Can't join with query with GROUP BY expression")
        GRDBPrecondition(rightQuery.havingExpression == nil, "Can't join with query with GROUP BY expression")
        GRDBPrecondition(rightQuery.limit == nil, "Can't join with query with limit")
        GRDBPrecondition(rightQuery.adapter == nil, "Support for right row adapter is not implemented")

        // SELECT * FROM left ... -> SELECT left.* FROM left ...
        let leftQuery = self.qualified(by: SQLSourceQualifier(alias: "left")) // table alias: we'll be smarter later

        // SELECT * FROM right ... -> SELECT right.* FROM right ...
        let rightQuery = rightQuery.qualified(by: SQLSourceQualifier(alias: "right")) // table alias: we'll be smarter later
        
        // Gather selections
        let joinedSelection = leftQuery.selection + rightQuery.selection
        
        // Join sources
        guard let leftSource = leftQuery.source else { fatalError("Support for sourceless joins is not implemented") }
        guard let rightSource = rightQuery.source else { fatalError("Support for sourceless joins is not implemented") }
        let joinedSource = SQLSource.joined(SQLSource.JoinDefinition(
            joinOp: joinOp,
            leftSource: leftSource,
            rightSource: rightSource,
            onExpression: rightQuery.whereExpression,
            mapping: mapping))
        
        // Gather orderings
        let joinedOrderings = leftQuery.eventuallyReversedOrderings + rightQuery.eventuallyReversedOrderings
        
        // Define row scopes
        let joinedAdapter = { (db: Database) -> RowAdapter in
            let leftCount = try leftQuery.numberOfColumns(db)
            let rightCount = try rightQuery.numberOfColumns(db)
            return ScopeAdapter([
                // Left columns start at index 0
                leftScope: RangeRowAdapter(0..<leftCount),
                // Right columns start after left columns
                rightScope: RangeRowAdapter(leftCount..<(leftCount + rightCount))])
        }
        
        return QueryInterfaceSelectQueryDefinition(
            select: joinedSelection,
            isDistinct: leftQuery.isDistinct, // TODO: test
            from: joinedSource,
            filter: leftQuery.whereExpression,
            groupBy: [],
            orderBy: joinedOrderings,
            isReversed: false,
            having: nil,
            limit: leftQuery.limit, // TODO: test
            adapter: joinedAdapter)
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
        
        let orderings = self.eventuallyReversedOrderings
        if !orderings.isEmpty {
            sql += " ORDER BY " + orderings.map { $0.orderingTermSQL(&arguments) }.joined(separator: ", ")
        }
        
        if let limit = limit {
            sql += " LIMIT " + limit.sql
        }
        
        return sql
    }
    
    var eventuallyReversedOrderings: [SQLOrderingTerm] {
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
                return [Column.rowID.desc]
            } else {
                return orderings.map { $0.reversed }
            }
        } else {
            return orderings
        }
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

indirect enum SQLSource {
    case table(name: String, qualifier: SQLSourceQualifier?)
    case query(query: QueryInterfaceSelectQueryDefinition, qualifier: SQLSourceQualifier?)
    case joined(JoinDefinition)
    
    struct JoinDefinition {
        let joinOp: SQLJoinOperator
        let leftSource: SQLSource
        let rightSource: SQLSource
        let onExpression: ((Database) throws -> SQLExpression)?
        let mapping: [(left: String, right: String)]
        
        func qualified(by qualifier: SQLSourceQualifier) -> JoinDefinition {
            return JoinDefinition(
                joinOp: joinOp,
                leftSource: leftSource.qualified(by: qualifier),
                rightSource: rightSource,
                onExpression: onExpression,
                mapping: mapping)
        }
        
        func sourceSQL(_ db: Database, _ arguments: inout StatementArguments?) throws -> String {
            // left JOIN right ON ...
            var sql = ""
            sql += try leftSource.sourceSQL(db, &arguments)
            sql += " \(joinOp.rawValue) "
            sql += try rightSource.sourceSQL(db, &arguments)
            
            // We're generating sql: sources must have been qualified by now
            let leftQualifier = leftSource.qualifier!
            let rightQualifier = rightSource.qualifier!
            
            var onClauses = mapping
                .map { arrow -> SQLExpression in
                    // right.leftId == left.id
                    let leftColumn = Column(arrow.left).qualified(by: leftQualifier)
                    let rightColumn = Column(arrow.right).qualified(by: rightQualifier)
                    return (rightColumn == leftColumn) }
            
            if let onExpression = try self.onExpression?(db) {
                // right.name = 'foo'
                onClauses.append(onExpression)
            }
            
            if !onClauses.isEmpty {
                let onClause = onClauses.suffix(from: 1).reduce(onClauses.first!, &&)
                sql += " ON " + onClause.expressionSQL(&arguments)
            }
            
            return sql
        }
    }
    
    var qualifier: SQLSourceQualifier? {
        switch self {
        case .table(_, let qualifier): return qualifier
        case .query(_, let qualifier): return qualifier
        case .joined(let joinDef): return joinDef.leftSource.qualifier
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
        case .joined(let joinDef):
            return try joinDef.sourceSQL(db, &arguments)
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
        case .joined(let joinDef):
            return .joined(joinDef.qualified(by: qualifier))
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
