import XCTest
#if GRDBCIPHER
    @testable import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    @testable import GRDBCustomSQLite
#else
    @testable import GRDB
#endif

private typealias Country = AssociationFixture.Country
private typealias CountryProfile = AssociationFixture.CountryProfile

class HasOneAssociationLeftJoinedTests: GRDBTestCase {
    
    func testSimplestRequest() throws {
        let dbQueue = try makeDatabaseQueue()
        try AssociationFixture().migrator.migrate(dbQueue)
        
        try dbQueue.inDatabase { db in
            let graph = try Country
                .leftJoined(with: Country.profile)
                .fetchAll(db)
            
            XCTAssertEqual(lastSQLQuery, "SELECT ...")
            
            assertMatch(graph, [])
        }
    }
    
    func testLeftRequestDerivation() throws {
        let dbQueue = try makeDatabaseQueue()
        try AssociationFixture().migrator.migrate(dbQueue)
        
        try dbQueue.inDatabase { db in
            do {
                // filter before leftJoined
                let graph = try Country
                    .filter(Column("code") != "AA")
                    .leftJoined(with: Country.profile)
                    .fetchAll(db)
                
                XCTAssertEqual(lastSQLQuery, "SELECT ...")
                
                assertMatch(graph, [])
            }
            
            do {
                // filter after leftJoined
                let graph = try Country
                    .leftJoined(with: Country.profile)
                    .filter(Column("code") != "AA")
                    .fetchAll(db)
                
                XCTAssertEqual(lastSQLQuery, "SELECT ...")
                
                assertMatch(graph, [])
            }
            
            do {
                // order before leftJoined
                let graph = try Country
                    .order(Column("code"))
                    .leftJoined(with: Country.profile)
                    .fetchAll(db)
                
                XCTAssertEqual(lastSQLQuery, "SELECT ...")
                
                assertMatch(graph, [])
            }
            
            do {
                // order after leftJoined
                let graph = try Country
                    .leftJoined(with: Country.profile)
                    .order(Column("code"))
                    .fetchAll(db)
                
                XCTAssertEqual(lastSQLQuery, "SELECT ...")
                
                assertMatch(graph, [])
            }
        }
    }
    
    func testRightRequestDerivation() throws {
        let dbQueue = try makeDatabaseQueue()
        try AssociationFixture().migrator.migrate(dbQueue)
        
        try dbQueue.inDatabase { db in
            do {
                let graph = try Country
                    .leftJoined(with: Country.profile.filter(Column("currency") == "EUR"))
                    .fetchAll(db)
                
                XCTAssertEqual(lastSQLQuery, "SELECT ...")
                
                assertMatch(graph, [])
            }
            
            do {
                let graph = try Country
                    .leftJoined(with: Country.profile.order(Column("area")))
                    .fetchAll(db)
                
                XCTAssertEqual(lastSQLQuery, "SELECT ...")
                
                assertMatch(graph, [])
            }
        }
    }
}
