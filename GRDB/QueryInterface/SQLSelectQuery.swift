// MARK: - SQLSelectQuery

/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
///
/// # Low Level Query Interface
///
/// SQLSelectQuery is the protocol for types that represent a full select query.
public protocol SQLSelectQuery : Request {
    
    /// This function is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    ///
    /// # Low Level Query Interface
    ///
    /// Returns the SQL string of the select query.
    ///
    /// When the arguments parameter is nil, any value must be written down as
    /// a literal in the returned SQL.
    ///
    /// When the arguments parameter is not nil, then values may be replaced by
    /// `?` or colon-prefixed tokens, and fed into arguments.
    func selectQuerySQL(_ db: Database, _ arguments: inout StatementArguments?) throws -> String
}


// MARK: - Request adoption

extension SQLSelectQuery {
    
    /// A tuple that contains a prepared statement that is ready to be
    /// executed, and an eventual row adapter.
    public func prepare(_ db: Database) throws -> (SelectStatement, RowAdapter?) {
        var arguments: StatementArguments? = StatementArguments()
        let sql = try selectQuerySQL(db, &arguments)
        let statement = try db.makeSelectStatement(sql)
        try statement.setArgumentsWithValidation(arguments!)
        return (statement, nil)
    }
}


// MARK: - QueryInterfaceSelectQueryDefinition

extension QueryInterfaceSelectQueryDefinition : SQLSelectQuery {
    /// This function is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    ///
    /// # Low Level Query Interface
    ///
    /// See SQLSelectQuery.selectQuerySQL(_:arguments:)
    public func selectQuerySQL(_ db: Database, _ arguments: inout StatementArguments?) throws -> String {
        return try sql(db, &arguments)
    }
    
    // Request protocol: customized count
    func fetchCount(_ db: Database) throws -> Int {
        return try Int.fetchOne(db, countQuery)!
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
