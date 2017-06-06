import XCTest
#if GRDBCIPHER
    import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    import GRDBCustomSQLite
#else
    import GRDB
#endif

class HasManyAssociationIncludingTests: GRDBTestCase {
    
    func testSimpleGraphRequest() throws {
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
        
        func assertEqual(_ graph: [(Parent, [Child])], _ expectedGraph: [(Parent, [Child])], file: StaticString = #file, line: UInt = #line) {
            XCTAssertEqual(graph.count, expectedGraph.count, file: file, line: line)
            for (pair, expectedPair) in zip(graph, expectedGraph) {
                let (parent, children) = pair
                let (expectedParent, expectedChildren) = expectedPair
                XCTAssertEqual(parent.id, expectedParent.id, file: file, line: line)
                XCTAssertEqual(parent.name, expectedParent.name, file: file, line: line)
                XCTAssertEqual(children.count, expectedChildren.count, file: file, line: line)
                for (child, expectedChild) in zip(children, expectedChildren) {
                    XCTAssertEqual(child.id, expectedChild.id, file: file, line: line)
                    XCTAssertEqual(child.parentId, expectedChild.parentId, file: file, line: line)
                    XCTAssertEqual(child.name, expectedChild.name, file: file, line: line)
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
        }
        
        try dbQueue.inDatabase { db in
            let graph = try Parent
                .including(Parent.children)
                .fetchAll(db)
            
            XCTAssertEqual(sqlQueries[sqlQueries.count - 2], "SELECT * FROM \"parents\"")
            XCTAssertEqual(sqlQueries[sqlQueries.count - 1], "SELECT * FROM \"children\" WHERE (\"parentId\" IN (1, 2))")
            
            let expectedGraph = [
                (Parent(row: ["id": 1, "name": "a"]), [
                    Child(row: ["id": 1, "parentId": 1, "name": "a"]),
                    Child(row: ["id": 2, "parentId": 1, "name": "b"]),
                    ]),
                (Parent(row: ["id": 2, "name": "b"]), [
                    Child(row: ["id": 3, "parentId": 2, "name": "a"]),
                    Child(row: ["id": 4, "parentId": 2, "name": "b"]),
                    ]),
            ]
            
            assertEqual(graph, expectedGraph)
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
            static let children = hasMany(Child.self)
            
            let id: Int64
            let name: String
            
            init(row: Row) {
                id = row.value(named: "id")
                name = row.value(named: "name")
            }
        }
        
        func assertEqual(_ graph: [(Parent, [Child])], _ expectedGraph: [(Parent, [Child])], file: StaticString = #file, line: UInt = #line) {
            XCTAssertEqual(graph.count, expectedGraph.count, file: file, line: line)
            for (pair, expectedPair) in zip(graph, expectedGraph) {
                let (parent, children) = pair
                let (expectedParent, expectedChildren) = expectedPair
                XCTAssertEqual(parent.id, expectedParent.id, file: file, line: line)
                XCTAssertEqual(parent.name, expectedParent.name, file: file, line: line)
                XCTAssertEqual(children.count, expectedChildren.count, file: file, line: line)
                for (child, expectedChild) in zip(children, expectedChildren) {
                    XCTAssertEqual(child.id, expectedChild.id, file: file, line: line)
                    XCTAssertEqual(child.parentId, expectedChild.parentId, file: file, line: line)
                    XCTAssertEqual(child.name, expectedChild.name, file: file, line: line)
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
        }
        
        try dbQueue.inDatabase { db in
            do {
                // filter before including
                let graph = try Parent
                    .filter(Column("name") == "a")
                    .including(Parent.children)
                    .fetchAll(db)
                
                XCTAssertEqual(sqlQueries[sqlQueries.count - 2], "SELECT * FROM \"parents\" WHERE (\"name\" = 'a')")
                XCTAssertEqual(sqlQueries[sqlQueries.count - 1], "SELECT * FROM \"children\" WHERE (\"parentId\" IN (1))")
                
                let expectedGraph = [
                    (Parent(row: ["id": 1, "name": "a"]), [
                        Child(row: ["id": 1, "parentId": 1, "name": "a"]),
                        Child(row: ["id": 2, "parentId": 1, "name": "b"]),
                        ]),
                    ]
                
                assertEqual(graph, expectedGraph)
            }
            
            do {
                // filter after including
                let graph = try Parent
                    .including(Parent.children)
                    .filter(Column("name") == "a")
                    .fetchAll(db)
                
                XCTAssertEqual(sqlQueries[sqlQueries.count - 2], "SELECT * FROM \"parents\" WHERE (\"name\" = 'a')")
                XCTAssertEqual(sqlQueries[sqlQueries.count - 1], "SELECT * FROM \"children\" WHERE (\"parentId\" IN (1))")
                
                let expectedGraph = [
                    (Parent(row: ["id": 1, "name": "a"]), [
                        Child(row: ["id": 1, "parentId": 1, "name": "a"]),
                        Child(row: ["id": 2, "parentId": 1, "name": "b"]),
                        ]),
                    ]
                
                assertEqual(graph, expectedGraph)
            }
            
            do {
                // order before including
                let graph = try Parent
                    .order(Column("name").desc)
                    .including(Parent.children)
                    .fetchAll(db)
                
                XCTAssertEqual(sqlQueries[sqlQueries.count - 2], "SELECT * FROM \"parents\" ORDER BY \"name\" DESC")
                XCTAssertEqual(sqlQueries[sqlQueries.count - 1], "SELECT * FROM \"children\" WHERE (\"parentId\" IN (2, 1))")
                
                let expectedGraph = [
                    (Parent(row: ["id": 2, "name": "b"]), [
                        Child(row: ["id": 3, "parentId": 2, "name": "a"]),
                        Child(row: ["id": 4, "parentId": 2, "name": "b"]),
                        ]),
                    (Parent(row: ["id": 1, "name": "a"]), [
                        Child(row: ["id": 1, "parentId": 1, "name": "a"]),
                        Child(row: ["id": 2, "parentId": 1, "name": "b"]),
                        ]),
                    ]
                
                assertEqual(graph, expectedGraph)
            }
            
            do {
                // order after including
                let graph = try Parent
                    .including(Parent.children)
                    .order(Column("name").desc)
                    .fetchAll(db)
                
                XCTAssertEqual(sqlQueries[sqlQueries.count - 2], "SELECT * FROM \"parents\" ORDER BY \"name\" DESC")
                XCTAssertEqual(sqlQueries[sqlQueries.count - 1], "SELECT * FROM \"children\" WHERE (\"parentId\" IN (2, 1))")
                
                let expectedGraph = [
                    (Parent(row: ["id": 2, "name": "b"]), [
                        Child(row: ["id": 3, "parentId": 2, "name": "a"]),
                        Child(row: ["id": 4, "parentId": 2, "name": "b"]),
                        ]),
                    (Parent(row: ["id": 1, "name": "a"]), [
                        Child(row: ["id": 1, "parentId": 1, "name": "a"]),
                        Child(row: ["id": 2, "parentId": 1, "name": "b"]),
                        ]),
                    ]
                
                assertEqual(graph, expectedGraph)
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
            static let children = hasMany(Child.self)
            static let orderedChildren = children.order(Column("name").desc)
            static let filteredChildren = children.filter(Column("name") == "a")
            
            let id: Int64
            let name: String
            
            init(row: Row) {
                id = row.value(named: "id")
                name = row.value(named: "name")
            }
        }
        
        func assertEqual(_ graph: [(Parent, [Child])], _ expectedGraph: [(Parent, [Child])], file: StaticString = #file, line: UInt = #line) {
            XCTAssertEqual(graph.count, expectedGraph.count, file: file, line: line)
            for (pair, expectedPair) in zip(graph, expectedGraph) {
                let (parent, children) = pair
                let (expectedParent, expectedChildren) = expectedPair
                XCTAssertEqual(parent.id, expectedParent.id, file: file, line: line)
                XCTAssertEqual(parent.name, expectedParent.name, file: file, line: line)
                XCTAssertEqual(children.count, expectedChildren.count, file: file, line: line)
                for (child, expectedChild) in zip(children, expectedChildren) {
                    XCTAssertEqual(child.id, expectedChild.id, file: file, line: line)
                    XCTAssertEqual(child.parentId, expectedChild.parentId, file: file, line: line)
                    XCTAssertEqual(child.name, expectedChild.name, file: file, line: line)
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
        }
        
        try dbQueue.inDatabase { db in
            do {
                // filtered children
                let graph = try Parent
                    .including(Parent.filteredChildren)
                    .fetchAll(db)
                
                XCTAssertEqual(sqlQueries[sqlQueries.count - 2], "SELECT * FROM \"parents\"")
                XCTAssertEqual(sqlQueries[sqlQueries.count - 1], "SELECT * FROM \"children\" WHERE ((\"name\" = 'a') AND (\"parentId\" IN (1, 2)))")
                
                let expectedGraph = [
                    (Parent(row: ["id": 1, "name": "a"]), [
                        Child(row: ["id": 1, "parentId": 1, "name": "a"]),
                        ]),
                    (Parent(row: ["id": 2, "name": "b"]), [
                        Child(row: ["id": 3, "parentId": 2, "name": "a"]),
                        ]),
                    ]
                
                assertEqual(graph, expectedGraph)
            }
            
            do {
                // ordered children
                let graph = try Parent
                    .including(Parent.orderedChildren)
                    .fetchAll(db)
                
                XCTAssertEqual(sqlQueries[sqlQueries.count - 2], "SELECT * FROM \"parents\"")
                XCTAssertEqual(sqlQueries[sqlQueries.count - 1], "SELECT * FROM \"children\" WHERE (\"parentId\" IN (1, 2)) ORDER BY \"name\" DESC")
                
                let expectedGraph = [
                    (Parent(row: ["id": 1, "name": "a"]), [
                        Child(row: ["id": 2, "parentId": 1, "name": "b"]),
                        Child(row: ["id": 1, "parentId": 1, "name": "a"]),
                        ]),
                    (Parent(row: ["id": 2, "name": "b"]), [
                        Child(row: ["id": 4, "parentId": 2, "name": "b"]),
                        Child(row: ["id": 3, "parentId": 2, "name": "a"]),
                        ]),
                    ]
                
                assertEqual(graph, expectedGraph)
            }
        }
    }
}
