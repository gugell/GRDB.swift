import XCTest
#if GRDBCIPHER
    import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    import GRDBCustomSQLite
#else
    import GRDB
#endif

class HasManyAssociationLeftJoinedTests: GRDBTestCase {
    
    // TODO: tests for left implicit row id, and compound keys
    // TODO: test fetchOne, fetchCursor
    // TODO: test sql snippets with table aliases
    
    func testSimplestRequest() throws {
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
        }
        
        struct Parent : TableMapping, RowConvertible {
            static let databaseTableName = "parents"
            let id: Int64
            let name: String
            
            init(row: Row) {
                id = row.value(named: "id")
                name = row.value(named: "name")
            }
            
            // The tested association
            static let children = hasMany(Child.self)
        }
        
        func assertEqual(_ graph: [(Parent, Child?)], _ expectedGraph: [(Parent, Child?)], file: StaticString = #file, line: UInt = #line) {
            XCTAssertEqual(graph.count, expectedGraph.count, file: file, line: line)
            for (pair, expectedPair) in zip(graph, expectedGraph) {
                let (parent, child) = pair
                let (expectedParent, expectedChild) = expectedPair
                XCTAssertEqual(parent.id, expectedParent.id, file: file, line: line)
                XCTAssertEqual(parent.name, expectedParent.name, file: file, line: line)
                switch (child, expectedChild) {
                case (nil, nil): break
                case (let child?, let expectedChild?):
                    XCTAssertEqual(child.id, expectedChild.id, file: file, line: line)
                    XCTAssertEqual(child.parentId, expectedChild.parentId, file: file, line: line)
                    XCTAssertEqual(child.name, expectedChild.name, file: file, line: line)
                default:
                    XCTFail()
                }
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
            
            try db.execute("INSERT INTO parents (id, name) VALUES (?, ?)", arguments: [1, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [1, 1, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [2, 1, "b"])
            try db.execute("INSERT INTO parents (id, name) VALUES (?, ?)", arguments: [2, "b"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [3, 2, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [4, 2, "b"])
            try db.execute("INSERT INTO parents (id, name) VALUES (?, ?)", arguments: [3, "a"])
        }
        
        try dbQueue.inDatabase { db in
            let graph = try Parent
                .leftJoined(with: Parent.children)
                .fetchAll(db)
            
            // TODO: check request & results
            XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\")")
        }
    }
    
    func testLeftRequestDerivation() throws {
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
        }
        
        struct Parent : TableMapping, RowConvertible {
            static let databaseTableName = "parents"
            let id: Int64
            let name: String
            
            init(row: Row) {
                id = row.value(named: "id")
                name = row.value(named: "name")
            }
            
            // The tested association
            static let children = hasMany(Child.self)
        }
        
        func assertEqual(_ graph: [(Parent, Child?)], _ expectedGraph: [(Parent, Child?)], file: StaticString = #file, line: UInt = #line) {
            XCTAssertEqual(graph.count, expectedGraph.count, file: file, line: line)
            for (pair, expectedPair) in zip(graph, expectedGraph) {
                let (parent, child) = pair
                let (expectedParent, expectedChild) = expectedPair
                XCTAssertEqual(parent.id, expectedParent.id, file: file, line: line)
                XCTAssertEqual(parent.name, expectedParent.name, file: file, line: line)
                switch (child, expectedChild) {
                case (nil, nil): break
                case (let child?, let expectedChild?):
                    XCTAssertEqual(child.id, expectedChild.id, file: file, line: line)
                    XCTAssertEqual(child.parentId, expectedChild.parentId, file: file, line: line)
                    XCTAssertEqual(child.name, expectedChild.name, file: file, line: line)
                default:
                    XCTFail()
                }
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
            
            try db.execute("INSERT INTO parents (id, name) VALUES (?, ?)", arguments: [1, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [1, 1, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [2, 1, "b"])
            try db.execute("INSERT INTO parents (id, name) VALUES (?, ?)", arguments: [2, "b"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [3, 2, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [4, 2, "b"])
            try db.execute("INSERT INTO parents (id, name) VALUES (?, ?)", arguments: [3, "a"])
        }
        
        // TODO: check request & results
        try dbQueue.inDatabase { db in
            do {
                // filter before including
                let graph = try Parent
                    .filter(Column("name") == "a")
                    .leftJoined(with: Parent.children)
                    .fetchAll(db)
                
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\") WHERE (\"left\".\"name\" = \'a\')")
            }
            
            do {
                // filter after including
                let graph = try Parent
                    .leftJoined(with: Parent.children)
                    .filter(Column("name") == "a")
                    .fetchAll(db)
                
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\") WHERE (\"left\".\"name\" = \'a\')")
            }
            
            do {
                // order before including
                let graph = try Parent
                    .order(Column("name").desc)
                    .leftJoined(with: Parent.children)
                    .fetchAll(db)
                
                // TODO: check request & results
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\") ORDER BY \"left\".\"name\" DESC")
            }
            
            do {
                // order after including
                let graph = try Parent
                    .leftJoined(with: Parent.children)
                    .order(Column("name").desc)
                    .fetchAll(db)
                
                // TODO: check request & results
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\") ORDER BY \"left\".\"name\" DESC")
            }
        }
    }
    
    func testRightRequestDerivation() throws {
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
        }
        
        struct Parent : TableMapping, RowConvertible {
            static let databaseTableName = "parents"
            let id: Int64
            let name: String
            
            init(row: Row) {
                id = row.value(named: "id")
                name = row.value(named: "name")
            }
            
            // The tested associations
            static let children = hasMany(Child.self)
            static let orderedChildren = children.order(Column("name").desc)
            static let filteredChildren = children.filter(Column("name") == "a")
        }
        
        func assertEqual(_ graph: [(Parent, Child?)], _ expectedGraph: [(Parent, Child?)], file: StaticString = #file, line: UInt = #line) {
            XCTAssertEqual(graph.count, expectedGraph.count, file: file, line: line)
            for (pair, expectedPair) in zip(graph, expectedGraph) {
                let (parent, child) = pair
                let (expectedParent, expectedChild) = expectedPair
                XCTAssertEqual(parent.id, expectedParent.id, file: file, line: line)
                XCTAssertEqual(parent.name, expectedParent.name, file: file, line: line)
                switch (child, expectedChild) {
                case (nil, nil): break
                case (let child?, let expectedChild?):
                    XCTAssertEqual(child.id, expectedChild.id, file: file, line: line)
                    XCTAssertEqual(child.parentId, expectedChild.parentId, file: file, line: line)
                    XCTAssertEqual(child.name, expectedChild.name, file: file, line: line)
                default:
                    XCTFail()
                }
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
            
            try db.execute("INSERT INTO parents (id, name) VALUES (?, ?)", arguments: [1, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [1, 1, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [2, 1, "b"])
            try db.execute("INSERT INTO parents (id, name) VALUES (?, ?)", arguments: [2, "b"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [3, 2, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [4, 2, "b"])
            try db.execute("INSERT INTO parents (id, name) VALUES (?, ?)", arguments: [3, "a"])
        }
        
        // TODO: check request & results
        try dbQueue.inDatabase { db in
            do {
                // filtered children
                let graph = try Parent
                    .leftJoined(with: Parent.filteredChildren)
                    .fetchAll(db)
                
                // TODO: check request & results
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON ((\"right\".\"parentId\" = \"left\".\"id\") AND (\"right\".\"name\" = \'a\'))")
            }
            
            do {
                // ordered children
                let graph = try Parent
                    .leftJoined(with: Parent.orderedChildren)
                    .fetchAll(db)
                
                // TODO: check request & results
                XCTAssertEqual(lastSQLQuery, "SELECT \"left\".*, \"right\".* FROM \"parents\" AS \"left\" LEFT JOIN \"children\" AS \"right\" ON (\"right\".\"parentId\" = \"left\".\"id\") ORDER BY \"right\".\"name\" DESC")
            }
        }
    }
}
