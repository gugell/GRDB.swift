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
    
    func testSingleColumnNoForeignKey() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
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
                try assertEqual(association.mapping(db), [(left: "parentId", right: "id")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parentId"], to: ["id"])
                try assertEqual(association.mapping(db), [(left: "parentId", right: "id")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
            }
        }
    }
    
    func testSingleColumnSingleForeignKey() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
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
                try assertEqual(association.mapping(db), [(left: "parentId", right: "id")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: "parentId")
                try assertEqual(association.mapping(db), [(left: "parentId", right: "id")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parentId"], to: ["id"])
                try assertEqual(association.mapping(db), [(left: "parentId", right: "id")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parentId\")")
            }
        }
    }
    
    func testSingleColumnSeveralForeignKeys() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
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
                try assertEqual(association.mapping(db), [(left: "parent1Id", right: "id")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent1Id\")")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parent1Id"], to: ["id"])
                try assertEqual(association.mapping(db), [(left: "parent1Id", right: "id")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent1Id\")")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: "parent2Id")
                try assertEqual(association.mapping(db), [(left: "parent2Id", right: "id")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent2Id\")")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parent2Id"], to: ["id"])
                try assertEqual(association.mapping(db), [(left: "parent2Id", right: "id")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON (\"right\".\"id\" = \"left\".\"parent2Id\")")
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
                try assertEqual(association.mapping(db), [(left: "parentA", right: "a"), (left: "parentB", right: "b")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parentA", "parentB"], to: ["a", "b"])
                try assertEqual(association.mapping(db), [(left: "parentA", right: "a"), (left: "parentB", right: "b")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
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
                try assertEqual(association.mapping(db), [(left: "parentA", right: "a"), (left: "parentB", right: "b")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: "parentA", "parentB")
                try assertEqual(association.mapping(db), [(left: "parentA", right: "a"), (left: "parentB", right: "b")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parentA", "parentB"], to: ["a", "b"])
                try assertEqual(association.mapping(db), [(left: "parentA", right: "a"), (left: "parentB", right: "b")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parentA\") AND (\"right\".\"b\" = \"left\".\"parentB\"))")
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
                try assertEqual(association.mapping(db), [(left: "parent1A", right: "a"), (left: "parent1B", right: "b")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent1A\") AND (\"right\".\"b\" = \"left\".\"parent1B\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parent1A", "parent1B"], to: ["a", "b"])
                try assertEqual(association.mapping(db), [(left: "parent1A", right: "a"), (left: "parent1B", right: "b")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent1A\") AND (\"right\".\"b\" = \"left\".\"parent1B\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: "parent2A", "parent2B")
                try assertEqual(association.mapping(db), [(left: "parent2A", right: "a"), (left: "parent2B", right: "b")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent2A\") AND (\"right\".\"b\" = \"left\".\"parent2B\"))")
            }
            do {
                let association = Child.belongsTo(Parent.self, from: ["parent2A", "parent2B"], to: ["a", "b"])
                try assertEqual(association.mapping(db), [(left: "parent2A", right: "a"), (left: "parent2B", right: "b")])
                _ = try Row.fetchAll(db, Child.all().joined(with: association))
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"children\" AS \"left\" JOIN \"parents\" AS \"right\" ON ((\"right\".\"a\" = \"left\".\"parent2A\") AND (\"right\".\"b\" = \"left\".\"parent2B\"))")
            }
        }
    }
}
