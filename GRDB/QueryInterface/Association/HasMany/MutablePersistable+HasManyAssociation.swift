extension MutablePersistable {
    public func makeRequest<Fetched>(_ association: HasManyAssociation<Self, Fetched>) -> QueryInterfaceRequest<Fetched> where Fetched: TableMapping {
        return association.makeRequest(from: self)
    }
    
    public func fetchCursor<Fetched>(_ db: Database, _ association: HasManyAssociation<Self, Fetched>) throws -> DatabaseCursor<Fetched> where Fetched: TableMapping & RowConvertible {
        return try association.makeRequest(from: self).fetchCursor(db)
    }
    
    public func fetchAll<Fetched>(_ db: Database, _ association: HasManyAssociation<Self, Fetched>) throws -> [Fetched] where Fetched: TableMapping & RowConvertible {
        return try association.makeRequest(from: self).fetchAll(db)
    }
    
    public func fetchOne<Fetched>(_ db: Database, _ association: HasManyAssociation<Self, Fetched>) throws -> Fetched? where Fetched: TableMapping & RowConvertible {
        return try association.makeRequest(from: self).fetchOne(db)
    }
}
