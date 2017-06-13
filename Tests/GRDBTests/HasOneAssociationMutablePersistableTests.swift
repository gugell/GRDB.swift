import XCTest
#if GRDBCIPHER
    import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    import GRDBCustomSQLite
#else
    import GRDB
#endif

private typealias Country = AssociationFixture.Country
private typealias CountryProfile = AssociationFixture.CountryProfile

class HasOneAssociationMutablePersistableTests: GRDBTestCase {
    
    func testRequest() throws {
        let dbQueue = try makeDatabaseQueue()
        try AssociationFixture().migrator.migrate(dbQueue)
        
        try dbQueue.inDatabase { db in
            
            do {
                let country = try Country.fetchOne(db, key: "FR")!
                let request = country.makeRequest(Country.profile)
                let profile = try request.fetchOne(db)
                XCTAssertEqual(lastSQLQuery, "SELECT ...")
                assertMatch(profile, [:])
            }
            
            do {
                let country = try Country.fetchOne(db, key: "AA")!
                let request = country.makeRequest(Country.profile)
                let profile = try request.fetchOne(db)
                XCTAssertEqual(lastSQLQuery, "SELECT ...")
                XCTAssertNil(profile)
            }
        }
    }
    
    func testFetchOne() throws {
        let dbQueue = try makeDatabaseQueue()
        try AssociationFixture().migrator.migrate(dbQueue)
        
        try dbQueue.inDatabase { db in
            
            do {
                // TODO: way to make the author non-optional?
                let country = try Country.fetchOne(db, key: "FR")!
                let profile = try country.fetchOne(db, Country.profile)
                XCTAssertEqual(lastSQLQuery, "SELECT ...")
                assertMatch(profile, [:])
            }
            
            do {
                let country = try Country.fetchOne(db, key: "AA")!
                let profile = try country.fetchOne(db, Country.profile)
                XCTAssertEqual(lastSQLQuery, "SELECT ...")
                XCTAssertNil(profile)
            }
        }
    }
}
