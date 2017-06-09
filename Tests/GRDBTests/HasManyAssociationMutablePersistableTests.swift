import XCTest
#if GRDBCIPHER
    import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    import GRDBCustomSQLite
#else
    import GRDB
#endif

private typealias Author = AssociationFixture.Author
private typealias Book = AssociationFixture.Book

class HasManyAssociationMutablePersistableTests: GRDBTestCase {
    
    // TODO: tests for left implicit row id, and compound keys
    // TODO: test fetchOne, fetchCursor
    
    func testBelongingTo() throws {
        let dbQueue = try makeDatabaseQueue()
        try AssociationFixture().migrator.migrate(dbQueue)
        
        try dbQueue.inDatabase { db in

            do {
                let author = try Author.filter(Column("name") == "J. M. Coetzee").fetchOne(db)!
                let request = Author.books.belonging(to: author)
                let books = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 2)")
                assertEqual(books, [
                    Book(row: ["id": 1, "authorId": author.id, "title": "Foe", "year": 0]),
                    Book(row: ["id": 2, "authorId": author.id, "title": "Three Stories", "year": 0]),
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let request = Author.books.belonging(to: author)
                let books = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 2)")
                assertEqual(books, [
                    Book(row: ["id": 3, "authorId": author.id, "title": "a", "year": 0]),
                    Book(row: ["id": 4, "authorId": author.id, "title": "b", "year": 0]),
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Gwendal Roué").fetchOne(db)!
                let request = Author.books.belonging(to: author)
                let books = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 3)")
                XCTAssertTrue(books.isEmpty)
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let request = Author.books.belonging(to: author).filter(Column("year") < 2000)
                let books = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE ((\"authorId\" = 1) AND (\"name\" = 'a'))")
                assertEqual(books, [
                    Book(row: ["id": 1, "authorId": author.id, "title": "a", "year": 0]),
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let request = Author.books.belonging(to: author).order(Column("title").desc)
                let books = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 1) ORDER BY \"name\" DESC")
                assertEqual(books, [
                    Book(row: ["id": 2, "authorId": author.id, "title": "b", "year": 0]),
                    Book(row: ["id": 1, "authorId": author.id, "title": "a", "year": 0]),
                    ])
            }
        }
    }
    
    func testFetchAll() throws {
        let dbQueue = try makeDatabaseQueue()
        try AssociationFixture().migrator.migrate(dbQueue)
        
        try dbQueue.inDatabase { db in
            
            do {
                let author = try Author.filter(Column("name") == "J. M. Coetzee").fetchOne(db)!
                let books = try author.fetchAll(db, Author.books)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 1)")
                assertEqual(books, [
                    Book(row: ["id": 1, "authorId": author.id, "title": "a", "year": 0]),
                    Book(row: ["id": 2, "authorId": author.id, "title": "b", "year": 0]),
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let books = try author.fetchAll(db, Author.books)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 2)")
                assertEqual(books, [
                    Book(row: ["id": 3, "authorId": author.id, "title": "a", "year": 0]),
                    Book(row: ["id": 4, "authorId": author.id, "title": "b", "year": 0]),
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Gwendal Roué").fetchOne(db)!
                let books = try author.fetchAll(db, Author.books)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 3)")
                XCTAssertTrue(books.isEmpty)
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let books = try author.fetchAll(db, Author.books.filter(Column("year") < 2000))
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE ((\"authorId\" = 1) AND (\"name\" = 'a'))")
                assertEqual(books, [
                    Book(row: ["id": 1, "authorId": author.id, "title": "a", "year": 0]),
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let books = try author.fetchAll(db, Author.books.order(Column("title").desc))
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 1) ORDER BY \"name\" DESC")
                assertEqual(books, [
                    Book(row: ["id": 2, "authorId": author.id, "title": "b", "year": 0]),
                    Book(row: ["id": 1, "authorId": author.id, "title": "a", "year": 0]),
                    ])
            }
        }
    }
}
