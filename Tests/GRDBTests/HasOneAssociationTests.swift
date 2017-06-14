import XCTest
#if GRDBCIPHER
    import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    import GRDBCustomSQLite
#else
    import GRDB
#endif

class HasOneAssociationTests: GRDBTestCase {
    
    func testSingleColumnNoForeignKeyNoPrimaryKey() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping, MutablePersistable {
            static let databaseTableName = "parents"
            func encode(to container: inout PersistenceContainer) {
                container["id"] = 1
                container["rowid"] = 2
            }
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
                let association = Parent.hasOne(Child.self, from: "parentId")
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"rowid\")")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"rowid\")")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE (\"parentId\" = 2)")
            }
            do {
                let association = Parent.hasOne(Child.self, from: ["parentId"], to: ["id"])
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE (\"parentId\" = 1)")
            }
        }
    }
    
    func testSingleColumnNoForeignKey() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping, MutablePersistable {
            static let databaseTableName = "parents"
            func encode(to container: inout PersistenceContainer) {
                container["id"] = 1
            }
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
                let association = Parent.hasOne(Child.self, from: "parentId")
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE (\"parentId\" = 1)")
            }
            do {
                let association = Parent.hasOne(Child.self, from: ["parentId"], to: ["id"])
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE (\"parentId\" = 1)")
            }
        }
    }
    
    func testSingleColumnSingleForeignKey() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping, MutablePersistable {
            static let databaseTableName = "parents"
            func encode(to container: inout PersistenceContainer) {
                container["id"] = 1
            }
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
                let association = Parent.hasOne(Child.self)
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE (\"parentId\" = 1)")
            }
            do {
                let association = Parent.hasOne(Child.self, from: "parentId")
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE (\"parentId\" = 1)")
            }
            do {
                let association = Parent.hasOne(Child.self, from: ["parentId"], to: ["id"])
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE (\"parentId\" = 1)")
            }
        }
    }
    
    func testSingleColumnSeveralForeignKeys() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping, MutablePersistable {
            static let databaseTableName = "parents"
            func encode(to container: inout PersistenceContainer) {
                container["id"] = 1
            }
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
                let association = Parent.hasOne(Child.self, from: "parent1Id")
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON (\"right\".\"parent1Id\" = \"left\".\"id\")")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parent1Id\" = \"left\".\"id\")")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE (\"parent1Id\" = 1)")
            }
            do {
                let association = Parent.hasOne(Child.self, from: ["parent1Id"], to: ["id"])
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON (\"right\".\"parent1Id\" = \"left\".\"id\")")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parent1Id\" = \"left\".\"id\")")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE (\"parent1Id\" = 1)")
            }
            do {
                let association = Parent.hasOne(Child.self, from: "parent2Id")
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON (\"right\".\"parent2Id\" = \"left\".\"id\")")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parent2Id\" = \"left\".\"id\")")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE (\"parent2Id\" = 1)")
            }
            do {
                let association = Parent.hasOne(Child.self, from: ["parent2Id"], to: ["id"])
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON (\"right\".\"parent2Id\" = \"left\".\"id\")")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parent2Id\" = \"left\".\"id\")")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE (\"parent2Id\" = 1)")
            }
        }
    }
    
    func testCompoundColumnNoForeignKeyNoPrimaryKey() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping, MutablePersistable {
            static let databaseTableName = "parents"
            func encode(to container: inout PersistenceContainer) {
                container["a"] = 1
                container["b"] = 2
            }
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
                let association = Parent.hasOne(Child.self, from: ["parentA", "parentB"], to: ["a", "b"])
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE ((\"parentA\" = 1) AND (\"parentB\" = 2))")
            }
        }
    }
    
    func testCompoundColumnNoForeignKey() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping, MutablePersistable {
            static let databaseTableName = "parents"
            func encode(to container: inout PersistenceContainer) {
                container["a"] = 1
                container["b"] = 2
            }
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
                let association = Parent.hasOne(Child.self, from: "parentA", "parentB")
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE ((\"parentA\" = 1) AND (\"parentB\" = 2))")
            }
            do {
                let association = Parent.hasOne(Child.self, from: ["parentA", "parentB"], to: ["a", "b"])
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE ((\"parentA\" = 1) AND (\"parentB\" = 2))")
            }
        }
    }
    
    func testCompoundColumnSingleForeignKey() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping, MutablePersistable {
            static let databaseTableName = "parents"
            func encode(to container: inout PersistenceContainer) {
                container["a"] = 1
                container["b"] = 2
            }
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
                let association = Parent.hasOne(Child.self)
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE ((\"parentA\" = 1) AND (\"parentB\" = 2))")
            }
            do {
                let association = Parent.hasOne(Child.self, from: "parentA", "parentB")
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE ((\"parentA\" = 1) AND (\"parentB\" = 2))")
            }
            do {
                let association = Parent.hasOne(Child.self, from: ["parentA", "parentB"], to: ["a", "b"])
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON ((\"right\".\"parentA\" = \"left\".\"a\") AND (\"right\".\"parentB\" = \"left\".\"b\"))")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE ((\"parentA\" = 1) AND (\"parentB\" = 2))")
            }
        }
    }
    
    func testCompoundColumnSeveralForeignKeys() throws {
        struct Child : TableMapping {
            static let databaseTableName = "children"
        }
        
        struct Parent : TableMapping, MutablePersistable {
            static let databaseTableName = "parents"
            func encode(to container: inout PersistenceContainer) {
                container["a"] = 1
                container["b"] = 2
            }
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
                let association = Parent.hasOne(Child.self, from: "parent1A", "parent1B")
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON ((\"right\".\"parent1A\" = \"left\".\"a\") AND (\"right\".\"parent1B\" = \"left\".\"b\"))")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON ((\"right\".\"parent1A\" = \"left\".\"a\") AND (\"right\".\"parent1B\" = \"left\".\"b\"))")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE ((\"parent1A\" = 1) AND (\"parent1B\" = 2))")
            }
            do {
                let association = Parent.hasOne(Child.self, from: ["parent1A", "parent1B"], to: ["a", "b"])
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON ((\"right\".\"parent1A\" = \"left\".\"a\") AND (\"right\".\"parent1B\" = \"left\".\"b\"))")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON ((\"right\".\"parent1A\" = \"left\".\"a\") AND (\"right\".\"parent1B\" = \"left\".\"b\"))")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE ((\"parent1A\" = 1) AND (\"parent1B\" = 2))")
            }
            do {
                let association = Parent.hasOne(Child.self, from: "parent2A", "parent2B")
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON ((\"right\".\"parent2A\" = \"left\".\"a\") AND (\"right\".\"parent2B\" = \"left\".\"b\"))")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON ((\"right\".\"parent2A\" = \"left\".\"a\") AND (\"right\".\"parent2B\" = \"left\".\"b\"))")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE ((\"parent2A\" = 1) AND (\"parent2B\" = 2))")
            }
            do {
                let association = Parent.hasOne(Child.self, from: ["parent2A", "parent2B"], to: ["a", "b"])
                try assertSQL(db, Parent.all().joined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" JOIN \"children\" AS \"right\" ON ((\"right\".\"parent2A\" = \"left\".\"a\") AND (\"right\".\"parent2B\" = \"left\".\"b\"))")
                try assertSQL(db, Parent.all().leftJoined(with: association), "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON ((\"right\".\"parent2A\" = \"left\".\"a\") AND (\"right\".\"parent2B\" = \"left\".\"b\"))")
                try assertSQL(db, Parent().makeRequest(association), "SELECT * FROM \"children\" WHERE ((\"parent2A\" = 1) AND (\"parent2B\" = 2))")
            }
        }
    }
}