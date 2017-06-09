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
                assertMatch(books, [
                    ["id": 1, "authorId": author.id, "title": "Foe", "year": 1986],
                    ["id": 2, "authorId": author.id, "title": "Three Stories", "year": 2014],
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let request = Author.books.belonging(to: author)
                let books = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 4)")
                assertMatch(books, [
                    ["id": 4, "authorId": author.id, "title": "New York 2140", "year": 2017],
                    ["id": 5, "authorId": author.id, "title": "2312", "year": 2012],
                    ["id": 6, "authorId": author.id, "title": "Blue Mars", "year": 1996],
                    ["id": 7, "authorId": author.id, "title": "Green Mars", "year": 1994],
                    ["id": 8, "authorId": author.id, "title": "Red Mars", "year": 1993],
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Gwendal Roué").fetchOne(db)!
                let request = Author.books.belonging(to: author)
                let books = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 1)")
                XCTAssertTrue(books.isEmpty)
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let request = Author.books.belonging(to: author).filter(Column("year") < 2000)
                let books = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE ((\"authorId\" = 4) AND (\"year\" < 2000))")
                assertMatch(books, [
                    ["id": 6, "authorId": author.id, "title": "Blue Mars", "year": 1996],
                    ["id": 7, "authorId": author.id, "title": "Green Mars", "year": 1994],
                    ["id": 8, "authorId": author.id, "title": "Red Mars", "year": 1993],
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let request = Author.books.belonging(to: author).order(Column("title").desc)
                let books = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 4) ORDER BY \"title\" DESC")
                assertMatch(books, [
                    ["id": 8, "authorId": author.id, "title": "Red Mars", "year": 1993],
                    ["id": 4, "authorId": author.id, "title": "New York 2140", "year": 2017],
                    ["id": 7, "authorId": author.id, "title": "Green Mars", "year": 1994],
                    ["id": 6, "authorId": author.id, "title": "Blue Mars", "year": 1996],
                    ["id": 5, "authorId": author.id, "title": "2312", "year": 2012],
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
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 2)")
                assertMatch(books, [
                    ["id": 1, "authorId": author.id, "title": "Foe", "year": 1986],
                    ["id": 2, "authorId": author.id, "title": "Three Stories", "year": 2014],
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let books = try author.fetchAll(db, Author.books)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 4)")
                assertMatch(books, [
                    ["id": 4, "authorId": author.id, "title": "New York 2140", "year": 2017],
                    ["id": 5, "authorId": author.id, "title": "2312", "year": 2012],
                    ["id": 6, "authorId": author.id, "title": "Blue Mars", "year": 1996],
                    ["id": 7, "authorId": author.id, "title": "Green Mars", "year": 1994],
                    ["id": 8, "authorId": author.id, "title": "Red Mars", "year": 1993],
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Gwendal Roué").fetchOne(db)!
                let books = try author.fetchAll(db, Author.books)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 1)")
                XCTAssertTrue(books.isEmpty)
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let books = try author.fetchAll(db, Author.books.filter(Column("year") < 2000))
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE ((\"year\" < 2000) AND (\"authorId\" = 4))")
                assertMatch(books, [
                    ["id": 6, "authorId": author.id, "title": "Blue Mars", "year": 1996],
                    ["id": 7, "authorId": author.id, "title": "Green Mars", "year": 1994],
                    ["id": 8, "authorId": author.id, "title": "Red Mars", "year": 1993],
                    ])
            }
            
            do {
                let author = try Author.filter(Column("name") == "Kim Stanley Robinson").fetchOne(db)!
                let books = try author.fetchAll(db, Author.books.order(Column("title").desc))
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"books\" WHERE (\"authorId\" = 4) ORDER BY \"title\" DESC")
                assertMatch(books, [
                    ["id": 8, "authorId": author.id, "title": "Red Mars", "year": 1993],
                    ["id": 4, "authorId": author.id, "title": "New York 2140", "year": 2017],
                    ["id": 7, "authorId": author.id, "title": "Green Mars", "year": 1994],
                    ["id": 6, "authorId": author.id, "title": "Blue Mars", "year": 1996],
                    ["id": 5, "authorId": author.id, "title": "2312", "year": 2012],
                    ])
            }
        }
    }
}
