import XCTest
#if GRDBCIPHER
    @testable import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    @testable import GRDBCustomSQLite
#else
    @testable import GRDB
#endif

class BelongsToAssociationTests: GRDBTestCase {
    
    func assertEqual(_ mapping: [(left: String, right: String)], _ expectedMapping: [(left: String, right: String)], file: StaticString = #file, line: UInt = #line) {
        XCTAssertEqual(mapping.count, expectedMapping.count, file: file, line: line)
        for (arrow, expectedArrow) in zip(mapping, expectedMapping) {
            XCTAssertEqual(arrow.left, expectedArrow.left, file: file, line: line)
            XCTAssertEqual(arrow.right, expectedArrow.right, file: file, line: line)
        }
    }
    
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
            
            // The tested association
            static let parent = belongsTo(Parent.self)
        }
        
        struct Parent : TableMapping, RowConvertible {
            static let databaseTableName = "parents"
            let id: Int64
            let name: String
            
            init(row: Row) {
                id = row.value(named: "id")
                name = row.value(named: "name")
            }
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
            let mapping = try Child.parent.mapping(db)
            assertEqual(mapping, [(left: "parentId", right: "id")])
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
            
            // The tested association
            static let parent = belongsTo(Parent.self, from: "parentId")
        }
        
        struct Parent : TableMapping, RowConvertible {
            static let databaseTableName = "parents"
            let id: Int64
            let name: String
            
            init(row: Row) {
                id = row.value(named: "id")
                name = row.value(named: "name")
            }
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
            let mapping = try Child.parent.mapping(db)
            assertEqual(mapping, [(left: "parentId", right: "id")])
        }
    }
}
