import XCTest
#if GRDBCIPHER
    @testable import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    @testable import GRDBCustomSQLite
#else
    @testable import GRDB
#endif

class HasManyAssociationForeignKeyTests: GRDBTestCase {
    
    // TODO: tests for left implicit row id, compound keys, and missing foreign key

    func testInferredForeignKey() throws {
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
        }
        
        try dbQueue.inDatabase { db in
            let foreignKey = try Parent.children.foreignKey(db)
            XCTAssertEqual(foreignKey.destinationTable, "parents")
            XCTAssertEqual(foreignKey.destinationColumns, ["id"])
            XCTAssertEqual(foreignKey.originColumns, ["parentId"])
        }
    }
    
    func testSingleRightColumn() throws {
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
            static let children = hasMany(Child.self, from: "parentId")
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
        }
        
        try dbQueue.inDatabase { db in
            let foreignKey = try Parent.children.foreignKey(db)
            XCTAssertEqual(foreignKey.destinationTable, "parents")
            XCTAssertEqual(foreignKey.destinationColumns, ["id"])
            XCTAssertEqual(foreignKey.originColumns, ["parentId"])
        }
    }
}
