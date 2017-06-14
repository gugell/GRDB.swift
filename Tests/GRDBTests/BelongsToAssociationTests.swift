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
    
    func testSingleColumnNoForeignKeyNoPrimaryKey() throws {
        struct Child : TableMapping, MutablePersistable {
            static let databaseTableName = "children"
            func encode(to container: inout PersistenceContainer) {
                container["parentId"] = 1
            }
        }
        
        struct Parent : TableMapping {
            static let databaseTableName = "parents"
        }
        
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            try db.create(table: "parents") { t in
                t.column("id", .integer)
            }
            try db.create(table: "children") { t in
                t.column("parentId", .integer)
            }
        }
        
        try dbQueue.inDatabase { db in
            do {
                let association = Child.belongsTo(Parent.self, from: "parentId")
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"rowid\" = \"left\".\"parentId\")")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON (\"right\".\"rowid\" = \"left\".\"parentId\")")
                try assertSQL(db, Child().makeRequest(association), "SELECT * FROM \"parents\" WHERE (\"rowid\" = 1)")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parentId"], to: ["id"])
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child().makeRequest(association), "SELECT * FROM \"parents\" WHERE (\"id\" = 1)")
            }
        }
    }
    
    func testSingleColumnNoForeignKey() throws {
        struct Child : TableMapping, MutablePersistable {
            static let databaseTableName = "children"
            func encode(to container: inout PersistenceContainer) {
                container["parentId"] = 1
            }
        }
        
        struct Parent : TableMapping {
            static let databaseTableName = "parents"
        }
        
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            try db.create(table: "parents") { t in
                t.column("id", .integer).primaryKey()
            }
            try db.create(table: "children") { t in
                t.column("parentId", .integer)
            }
        }
        
        try dbQueue.inDatabase { db in
            do {
                let association = Child.belongsTo(Parent.self, from: "parentId")
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child().makeRequest(association), "SELECT * FROM \"parents\" WHERE (\"id\" = 1)")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parentId"], to: ["id"])
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child().makeRequest(association), "SELECT * FROM \"parents\" WHERE (\"id\" = 1)")
            }
        }
    }
    
    func testSingleColumnSingleForeignKey() throws {
        struct Child : TableMapping, MutablePersistable {
            static let databaseTableName = "children"
            func encode(to container: inout PersistenceContainer) {
                container["parentId"] = 1
            }
        }
        
        struct Parent : TableMapping {
            static let databaseTableName = "parents"
        }
        
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            try db.create(table: "parents") { t in
                t.column("id", .integer).primaryKey()
            }
            try db.create(table: "children") { t in
                t.column("parentId", .integer).references("parents")
            }
        }
        
        try dbQueue.inDatabase { db in
            do {
                let association = Child.belongsTo(Parent.self)
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child().makeRequest(association), "SELECT * FROM \"parents\" WHERE (\"id\" = 1)")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: "parentId")
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child().makeRequest(association), "SELECT * FROM \"parents\" WHERE (\"id\" = 1)")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parentId"], to: ["id"])
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
                try assertSQL(db, Child().makeRequest(association), "SELECT * FROM \"parents\" WHERE (\"id\" = 1)")
            }
        }
    }
    
    func testSingleColumnSeveralForeignKeys() throws {
        struct Child : TableMapping, MutablePersistable {
            static let databaseTableName = "children"
            func encode(to container: inout PersistenceContainer) {
                container["parent1Id"] = 1
                container["parent2Id"] = 2
            }
        }
        
        struct Parent : TableMapping {
            static let databaseTableName = "parents"
        }
        
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            try db.create(table: "parents") { t in
                t.column("id", .integer).primaryKey()
            }
            try db.create(table: "children") { t in
                t.column("parent1Id", .integer).references("parents")
                t.column("parent2Id", .integer).references("parents")
            }
        }
        
        try dbQueue.inDatabase { db in
            do {
                let association = Child.belongsTo(Parent.self, from: "parent1Id")
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent1Id\")")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent1Id\")")
                try assertSQL(db, Child().makeRequest(association), "SELECT * FROM \"parents\" WHERE (\"id\" = 1)")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parent1Id"], to: ["id"])
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent1Id\")")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent1Id\")")
                try assertSQL(db, Child().makeRequest(association), "SELECT * FROM \"parents\" WHERE (\"id\" = 1)")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: "parent2Id")
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent2Id\")")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent2Id\")")
                try assertSQL(db, Child().makeRequest(association), "SELECT * FROM \"parents\" WHERE (\"id\" = 2)")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parent2Id"], to: ["id"])
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent2Id\")")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent2Id\")")
                try assertSQL(db, Child().makeRequest(association), "SELECT * FROM \"parents\" WHERE (\"id\" = 2)")
            }
        }
    }
    
    func testCompoundColumnNoForeignKeyNoPrimaryKey() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping {
            static let databaseTableName = "parents"
        }
        
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            try db.create(table: "parents") { t in
                t.column("a", .integer)
                t.column("b", .integer)
            }
            try db.create(table: "children") { t in
                t.column("parentA", .integer)
                t.column("parentB", .integer)
            }
        }
        
        try dbQueue.inDatabase { db in
            do {
                let association = Child.belongsTo(Parent.self, from: ["parentA", "parentB"], to: ["a", "b"])
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
            }
        }
    }
    
    func testCompoundColumnNoForeignKey() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping {
            static let databaseTableName = "parents"
        }
        
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            try db.create(table: "parents") { t in
                t.column("a", .integer)
                t.column("b", .integer)
                t.primaryKey(["a", "b"])
            }
            try db.create(table: "children") { t in
                t.column("parentA", .integer)
                t.column("parentB", .integer)
            }
        }
        
        try dbQueue.inDatabase { db in
            do {
                let association = Child.belongsTo(Parent.self, from: "parentA", "parentB")
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parentA", "parentB"], to: ["a", "b"])
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
            }
        }
    }
    
    func testCompoundColumnSingleForeignKey() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping {
            static let databaseTableName = "parents"
        }
        
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            try db.create(table: "parents") { t in
                t.column("a", .integer)
                t.column("b", .integer)
                t.primaryKey(["a", "b"])
            }
            try db.create(table: "children") { t in
                t.column("parentA", .integer)
                t.column("parentB", .integer)
                t.foreignKey(["parentA", "parentB"], references: "parents")
            }
        }
        
        try dbQueue.inDatabase { db in
            do {
                let association = Child.belongsTo(Parent.self)
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: "parentA", "parentB")
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parentA", "parentB"], to: ["a", "b"])
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
            }
        }
    }
    
    func testCompoundColumnSeveralForeignKeys() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping {
            static let databaseTableName = "parents"
        }
        
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            try db.create(table: "parents") { t in
                t.column("a", .integer)
                t.column("b", .integer)
                t.primaryKey(["a", "b"])
            }
            try db.create(table: "children") { t in
                t.column("parent1A", .integer)
                t.column("parent1B", .integer)
                t.column("parent2A", .integer)
                t.column("parent2B", .integer)
                t.foreignKey(["parent1A", "parent1B"], references: "parents")
                t.foreignKey(["parent2A", "parent2B"], references: "parents")
            }
        }
        
        try dbQueue.inDatabase { db in
            do {
                let association = Child.belongsTo(Parent.self, from: "parent1A", "parent1B")
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent1A\") AND (\"right\".\"b\" = \"left\".\"parent1B\"))")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent1A\") AND (\"right\".\"b\" = \"left\".\"parent1B\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parent1A", "parent1B"], to: ["a", "b"])
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent1A\") AND (\"right\".\"b\" = \"left\".\"parent1B\"))")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent1A\") AND (\"right\".\"b\" = \"left\".\"parent1B\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: "parent2A", "parent2B")
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent2A\") AND (\"right\".\"b\" = \"left\".\"parent2B\"))")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent2A\") AND (\"right\".\"b\" = \"left\".\"parent2B\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parent2A", "parent2B"], to: ["a", "b"])
                try assertSQL(db, Child.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent2A\") AND (\"right\".\"b\" = \"left\".\"parent2B\"))")
                try assertSQL(db, Child.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" LEFT JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent2A\") AND (\"right\".\"b\" = \"left\".\"parent2B\"))")
            }
        }
    }
}
