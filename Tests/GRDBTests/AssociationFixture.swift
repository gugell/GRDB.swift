import XCTest
#if GRDBCIPHER
    @testable import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    @testable import GRDBCustomSQLite
#else
    @testable import GRDB
#endif

struct AssociationFixture {
    
    struct Book : TableMapping, RowConvertible, MutablePersistable {
        static let databaseTableName = "books"
        let id: Int64?
        let authorId: Int64
        let title: String
        let year: Int
        
        init(row: Row) {
            id = row.value(named: "id")
            authorId = row.value(named: "authorId")
            title = row.value(named: "title")
            year = row.value(named: "year")
        }
        
        func encode(to container: inout PersistenceContainer) {
            container["id"] = id
            container["authorId"] = authorId
            container["title"] = title
            container["year"] = year
        }
        
        static let author = belongsTo(Author.self)
    }
    
    struct Author : TableMapping, RowConvertible, MutablePersistable {
        static let databaseTableName = "authors"
        let id: Int64?
        let name: String
        
        init(row: Row) {
            id = row.value(named: "id")
            name = row.value(named: "name")
        }
        
        func encode(to container: inout PersistenceContainer) {
            container["id"] = id
            container["name"] = name
        }
        
        static let books = hasMany(Book.self)
    }
    
    var migrator: DatabaseMigrator {
        var migrator = DatabaseMigrator()
        
        migrator.registerMigration("fixtures") { db in
            try db.create(table: "authors") { t in
                t.column("id", .integer).primaryKey()
                t.column("name", .text).notNull()
            }
            try db.execute("INSERT INTO authors (name) VALUES (?)", arguments: ["Gwendal Roué"])
            try db.execute("INSERT INTO authors (name) VALUES (?)", arguments: ["J. M. Coetzee"])
            let coetzeeId = db.lastInsertedRowID
            try db.execute("INSERT INTO authors (name) VALUES (?)", arguments: ["Herman Melville"])
            let melvilleId = db.lastInsertedRowID
            try db.execute("INSERT INTO authors (name) VALUES (?)", arguments: ["Kim Stanley Robinson"])
            let robinsonId = db.lastInsertedRowID
            
            try db.create(table: "books") { t in
                t.column("id", .integer).primaryKey()
                t.column("authorId", .integer).notNull().references("authors")
                t.column("title", .text).notNull()
                t.column("year", .integer).notNull()
            }
            
            try db.execute("INSERT INTO books (authorId, title, year) VALUES (?, ?, ?)", arguments: [coetzeeId, "Foe", 1986])
            try db.execute("INSERT INTO books (authorId, title, year) VALUES (?, ?, ?)", arguments: [coetzeeId, "Three Stories", 2014])
            try db.execute("INSERT INTO books (authorId, title, year) VALUES (?, ?, ?)", arguments: [melvilleId, "Moby-Dick", 1851])
            try db.execute("INSERT INTO books (authorId, title, year) VALUES (?, ?, ?)", arguments: [robinsonId, "New York 2140", 2017])
            try db.execute("INSERT INTO books (authorId, title, year) VALUES (?, ?, ?)", arguments: [robinsonId, "2312", 2012])
            try db.execute("INSERT INTO books (authorId, title, year) VALUES (?, ?, ?)", arguments: [robinsonId, "Blue Mars", 1996])
            try db.execute("INSERT INTO books (authorId, title, year) VALUES (?, ?, ?)", arguments: [robinsonId, "Green Mars", 1994])
            try db.execute("INSERT INTO books (authorId, title, year) VALUES (?, ?, ?)", arguments: [robinsonId, "Red Mars", 1993])
        }
        
        return migrator
    }
}

extension GRDBTestCase {
    func assertEqual<Left, Right>(_ pair: (left: Left, right: Right), _ expectedPair: (left: Left, right: Right), file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        assertEqual(pair.left, expectedPair.left, file: file, line: line)
        assertEqual(pair.right, expectedPair.right, file: file, line: line)
    }
    
    func assertEqual<Left, Right>(_ graph: [(left: Left, right: Right)], _ expectedGraph: [(left: Left, right: Right)], file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        XCTAssertEqual(graph.count, expectedGraph.count, file: file, line: line)
        for (pair, expectedPair) in zip(graph, expectedGraph) {
            assertEqual(pair, expectedPair, file: file, line: line)
        }
    }
    
    func assertEqual<Left, Right>(_ pair: (left: Left, right: Right?), _ expectedPair: (left: Left, right: Right?), file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        assertEqual(pair.left, expectedPair.left, file: file, line: line)
        assertEqual(pair.right, expectedPair.right, file: file, line: line)
    }
    
    func assertEqual<Left, Right>(_ graph: [(left: Left, right: Right?)], _ expectedGraph: [(left: Left, right: Right?)], file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        XCTAssertEqual(graph.count, expectedGraph.count, file: file, line: line)
        for (pair, expectedPair) in zip(graph, expectedGraph) {
            assertEqual(pair, expectedPair, file: file, line: line)
        }
    }
    
    func assertEqual<Left, Right>(_ pair: (left: Left, right: [Right]), _ expectedPair: (left: Left, right: [Right]), file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        assertEqual(pair.left, expectedPair.left, file: file, line: line)
        assertEqual(pair.right, expectedPair.right, file: file, line: line)
    }
    
    func assertEqual<Left, Right>(_ graph: [(left: Left, right: [Right])], _ expectedGraph: [(left: Left, right: [Right])], file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        XCTAssertEqual(graph.count, expectedGraph.count, file: file, line: line)
        for (pair, expectedPair) in zip(graph, expectedGraph) {
            assertEqual(pair, expectedPair, file: file, line: line)
        }
    }
    
    func assertEqual<T>(_ records: [T], _ expectedRecords: [T], file: StaticString = #file, line: UInt = #line) where T: MutablePersistable {
        XCTAssertEqual(records.count, expectedRecords.count, file: file, line: line)
        for (record, expectedRecords) in zip(records, expectedRecords) {
            assertEqual(record, expectedRecords, file: file, line: line)
        }
    }
    
    func assertEqual<T>(_ record: T?, _ expectedRecord: T?, file: StaticString = #file, line: UInt = #line) where T: MutablePersistable {
        assertEqual(record.map { PersistenceContainer($0) }, expectedRecord.map { PersistenceContainer($0) }, file: file, line: line)
    }
    
    func assertEqual(_ container: PersistenceContainer?, _ expectedContainer: PersistenceContainer?, file: StaticString = #file, line: UInt = #line) {
        switch (container, expectedContainer) {
        case (let container?, let expectedContainer?):
            XCTAssertEqual(container.columns, expectedContainer.columns, file: file, line: line)
            XCTAssertEqual(container.values.map { $0?.databaseValue ?? .null } , expectedContainer.values.map { $0?.databaseValue ?? .null }, file: file, line: line)
        case (nil, nil):
            break
        default:
            XCTFail("not equal", file: file, line: line)
        }
    }
}
