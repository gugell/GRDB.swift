import XCTest
#if GRDBCIPHER
    import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    import GRDBCustomSQLite
#else
    import GRDB
#endif

class HasManyAssociationBelongingToTests: GRDBTestCase {
    
    // TODO: tests for left implicit row id, and compound keys
    // TODO: test fetchOne, fetchCursor
    
    func testBelongingTo() throws {
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
        
        struct Parent : MutablePersistable, RowConvertible {
            static let databaseTableName = "parents"
            var id: Int64?
            let name: String
            
            init(id: Int64?, name: String) {
                self.id = id
                self.name = name
            }
            
            init(row: Row) {
                id = row.value(named: "id")
                name = row.value(named: "name")
            }
            
            func encode(to container: inout PersistenceContainer) {
                container["id"] = id
                container["name"] = name
            }
            
            mutating func didInsert(with rowID: Int64, for column: String?) {
                id = rowID
            }
            
            // The tested association
            static let children = hasMany(Child.self)
        }
        
        func assertEqual(_ children: [Child], _ expectedChildren: [Child], file: StaticString = #file, line: UInt = #line) {
            XCTAssertEqual(children.count, expectedChildren.count, file: file, line: line)
            for (child, expectedChild) in zip(children, expectedChildren) {
                XCTAssertEqual(child.id, expectedChild.id, file: file, line: line)
                XCTAssertEqual(child.parentId, expectedChild.parentId, file: file, line: line)
                XCTAssertEqual(child.name, expectedChild.name, file: file, line: line)
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
            
            var a = Parent(id: nil, name: "a")
            try a.insert(db)
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [1, a.id, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [2, a.id, "b"])
            var b = Parent(id: nil, name: "b")
            try b.insert(db)
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [3, b.id, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [4, b.id, "b"])
            var c = Parent(id: nil, name: "a")
            try c.insert(db)

            do {
                let request = Parent.children.belonging(to: a)
                let children = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"children\" WHERE (\"parentId\" = 1)")
                assertEqual(children, [
                    Child(row: ["id": 1, "parentId": a.id, "name": "a"]),
                    Child(row: ["id": 2, "parentId": a.id, "name": "b"]),
                    ])
            }
            
            do {
                let request = Parent.children.belonging(to: b)
                let children = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"children\" WHERE (\"parentId\" = 2)")
                assertEqual(children, [
                    Child(row: ["id": 3, "parentId": b.id, "name": "a"]),
                    Child(row: ["id": 4, "parentId": b.id, "name": "b"]),
                    ])
            }
            
            do {
                let request = Parent.children.belonging(to: c)
                let children = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"children\" WHERE (\"parentId\" = 3)")
                XCTAssertTrue(children.isEmpty)
            }
            
            do {
                let request = Parent.children.belonging(to: a).filter(Column("name") == "a")
                let children = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"children\" WHERE ((\"parentId\" = 1) AND (\"name\" = 'a'))")
                assertEqual(children, [
                    Child(row: ["id": 1, "parentId": a.id, "name": "a"]),
                    ])
            }
            
            do {
                let request = Parent.children.belonging(to: a).order(Column("name").desc)
                let children = try request.fetchAll(db)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"children\" WHERE (\"parentId\" = 1) ORDER BY \"name\" DESC")
                assertEqual(children, [
                    Child(row: ["id": 2, "parentId": a.id, "name": "b"]),
                    Child(row: ["id": 1, "parentId": a.id, "name": "a"]),
                    ])
            }
        }
    }
    
    func testFetchAll() throws {
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
        
        struct Parent : MutablePersistable, RowConvertible {
            static let databaseTableName = "parents"
            var id: Int64?
            let name: String
            
            init(id: Int64?, name: String) {
                self.id = id
                self.name = name
            }
            
            init(row: Row) {
                id = row.value(named: "id")
                name = row.value(named: "name")
            }
            
            func encode(to container: inout PersistenceContainer) {
                container["id"] = id
                container["name"] = name
            }
            
            mutating func didInsert(with rowID: Int64, for column: String?) {
                id = rowID
            }
            
            // The tested association
            static let children = hasMany(Child.self)
        }
        
        func assertEqual(_ children: [Child], _ expectedChildren: [Child], file: StaticString = #file, line: UInt = #line) {
            XCTAssertEqual(children.count, expectedChildren.count, file: file, line: line)
            for (child, expectedChild) in zip(children, expectedChildren) {
                XCTAssertEqual(child.id, expectedChild.id, file: file, line: line)
                XCTAssertEqual(child.parentId, expectedChild.parentId, file: file, line: line)
                XCTAssertEqual(child.name, expectedChild.name, file: file, line: line)
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
            
            var a = Parent(id: nil, name: "a")
            try a.insert(db)
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [1, a.id, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [2, a.id, "b"])
            var b = Parent(id: nil, name: "b")
            try b.insert(db)
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [3, b.id, "a"])
            try db.execute("INSERT INTO children (id, parentId, name) VALUES (?, ?, ?)", arguments: [4, b.id, "b"])
            var c = Parent(id: nil, name: "a")
            try c.insert(db)
            
            do {
                let children = try a.fetchAll(db, Parent.children)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"children\" WHERE (\"parentId\" = 1)")
                assertEqual(children, [
                    Child(row: ["id": 1, "parentId": a.id, "name": "a"]),
                    Child(row: ["id": 2, "parentId": a.id, "name": "b"]),
                    ])
            }
            
            do {
                let children = try b.fetchAll(db, Parent.children)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"children\" WHERE (\"parentId\" = 2)")
                assertEqual(children, [
                    Child(row: ["id": 3, "parentId": b.id, "name": "a"]),
                    Child(row: ["id": 4, "parentId": b.id, "name": "b"]),
                    ])
            }
            
            do {
                let children = try c.fetchAll(db, Parent.children)
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"children\" WHERE (\"parentId\" = 3)")
                XCTAssertTrue(children.isEmpty)
            }
            
            do {
                let children = try a.fetchAll(db, Parent.children.filter(Column("name") == "a"))
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"children\" WHERE ((\"name\" = 'a') AND (\"parentId\" = 1))")
                assertEqual(children, [
                    Child(row: ["id": 1, "parentId": a.id, "name": "a"]),
                    ])
            }
            
            do {
                let children = try a.fetchAll(db, Parent.children.order(Column("name").desc))
                XCTAssertEqual(lastSQLQuery, "SELECT * FROM \"children\" WHERE (\"parentId\" = 1) ORDER BY \"name\" DESC")
                assertEqual(children, [
                    Child(row: ["id": 2, "parentId": a.id, "name": "b"]),
                    Child(row: ["id": 1, "parentId": a.id, "name": "a"]),
                    ])
            }
        }
    }
}
