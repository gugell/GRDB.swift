import XCTest
#if GRDBCIPHER
    import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    import GRDBCustomSQLite
#else
    import GRDB
#endif

class HasManyAssociationJoinedTests: GRDBTestCase {
    
    // TODO: tests for left implicit row id, and compound keys
    
    func testSimplestRequest() throws {
        struct Child : TableMapping, RowConvertible {
            static let databaseTableName = "children"
            let id: Int64
            let parentId: Int64
            let name: String
            
            init(row: Row) {
                id = row.value(named: "id")
                parentId = row.value(named: "parentId")
                name = row.value(named: "name")
            }
        }
        
        struct Parent : TableMapping, RowConvertible {
            static let databaseTableName = "parents"
            let id: Int64
            let name: String
            
            init(row: Row) {
                id = row.value(named: "id")
                name = row.value(named: "name")
            }
            
            // The tested association
            static let children = hasMany(Child.self)
        }
        
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            try db.create(table: "parents") { t in
                t.column("id", .integer).primaryKey()
                t.column("name", .text)
            }
            try db.create(table: "children") { t in
                t.column("id", .integer).primaryKey()
                t.column("parentId", .integer).references("parents")
                t.column("name", .text)
            }
            
            try db.execute("INSERT INTO parents (id, name) VALUES (?, ?)", arguments: [1, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [1, 1, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [2, 1, "b"])
            try db.execute("INSERT INTO parents (id, name) VALUES (?, ?)", arguments: [2, "b"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [3, 2, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [4, 2, "b"])
        }
        
        try dbQueue.inDatabase { db in
            let graph = try Parent
                .joined(with: Parent.children)
                .fetchAll(db)
            
            XCTAssertEqual(sqlQueries[sqlQueries.count - 2], "SELECT * FROM \"parents\" JOIN \"children\" ON (\"children\".\"parentId\" = \"parents\".\"id\")")
        }
    }
}
