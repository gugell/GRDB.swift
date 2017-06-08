extension MutablePersistable {
    public func fetchCursor<Fetched>(_ db: Database, _ association: HasManyAssociation<Self, Fetched>) throws -> DatabaseCursor<Fetched> where Fetched: TableMapping & RowConvertible {
        return try association.belonging(to: self).fetchCursor(db)
    }
    
    public func fetchAll<Fetched>(_ db: Database, _ association: HasManyAssociation<Self, Fetched>) throws -> [Fetched] where Fetched: TableMapping & RowConvertible {
        return try association.belonging(to: self).fetchAll(db)
    }
    
    public func fetchOne<Fetched>(_ db: Database, _ association: HasManyAssociation<Self, Fetched>) throws -> Fetched? where Fetched: TableMapping & RowConvertible {
        return try association.belonging(to: self).fetchOne(db)
    }
}
