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
        let birthYear: Int
        
        init(row: Row) {
            id = row.value(named: "id")
            name = row.value(named: "name")
            birthYear = row.value(named: "birthYear")
        }
        
        func encode(to container: inout PersistenceContainer) {
            container["id"] = id
            container["name"] = name
            container["birthYear"] = birthYear
        }
        
        static let books = hasMany(Book.self)
    }
    
    var migrator: DatabaseMigrator {
        var migrator = DatabaseMigrator()
        
        migrator.registerMigration("fixtures") { db in
            try db.create(table: "authors") { t in
                t.column("id", .integer).primaryKey()
                t.column("name", .text).notNull()
                t.column("birthYear", .integer).notNull()
            }
            try db.execute("INSERT INTO authors (name, birthYear) VALUES (?, ?)", arguments: ["Gwendal Roué", 1973])
            try db.execute("INSERT INTO authors (name, birthYear) VALUES (?, ?)", arguments: ["J. M. Coetzee", 1940])
            let coetzeeId = db.lastInsertedRowID
            try db.execute("INSERT INTO authors (name, birthYear) VALUES (?, ?)", arguments: ["Herman Melville", 1819])
            let melvilleId = db.lastInsertedRowID
            try db.execute("INSERT INTO authors (name, birthYear) VALUES (?, ?)", arguments: ["Kim Stanley Robinson", 1952])
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

// TODO: move into general GRDBTestCase
extension GRDBTestCase {
    func assertMatch<Left, Right>(_ pair: (Left, Right), _ expectedPair: (Row, Row), file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        assertMatch(pair.0, expectedPair.0, file: file, line: line)
        assertMatch(pair.1, expectedPair.1, file: file, line: line)
    }
    
    func assertMatch<Left, Right>(_ graph: [(Left, Right)], _ expectedGraph: [(Row, Row)], file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        XCTAssertEqual(graph.count, expectedGraph.count, "count mismatch for \(expectedGraph)", file: file, line: line)
        for (pair, expectedPair) in zip(graph, expectedGraph) {
            assertMatch(pair, expectedPair, file: file, line: line)
        }
    }
    
    func assertMatch<Left, Right>(_ pair: (Left, Right?), _ expectedPair: (Row, Row?), file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        assertMatch(pair.0, expectedPair.0, file: file, line: line)
        assertMatch(pair.1, expectedPair.1, file: file, line: line)
    }
    
    func assertMatch<Left, Right>(_ graph: [(Left, Right?)], _ expectedGraph: [(Row, Row?)], file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        XCTAssertEqual(graph.count, expectedGraph.count, "count mismatch for \(expectedGraph)", file: file, line: line)
        for (pair, expectedPair) in zip(graph, expectedGraph) {
            assertMatch(pair, expectedPair, file: file, line: line)
        }
    }
    
    func assertMatch<Left, Right>(_ pair: (Left, [Right]), _ expectedPair: (Row, [Row]), file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        assertMatch(pair.0, expectedPair.0, file: file, line: line)
        assertMatch(pair.1, expectedPair.1, file: file, line: line)
    }
    
    func assertMatch<Left, Right>(_ graph: [(Left, [Right])], _ expectedGraph: [(Row, [Row])], file: StaticString = #file, line: UInt = #line) where Left: MutablePersistable, Right: MutablePersistable {
        XCTAssertEqual(graph.count, expectedGraph.count, "count mismatch for \(expectedGraph)", file: file, line: line)
        for (pair, expectedPair) in zip(graph, expectedGraph) {
            assertMatch(pair, expectedPair, file: file, line: line)
        }
    }
    
    func assertMatch<T>(_ records: [T], _ expectedRows: [Row], file: StaticString = #file, line: UInt = #line) where T: MutablePersistable {
        XCTAssertEqual(records.count, expectedRows.count, "count mismatch for \(expectedRows)", file: file, line: line)
        for (record, expectedRow) in zip(records, expectedRows) {
            assertMatch(record, expectedRow, file: file, line: line)
        }
    }
}
