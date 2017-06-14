extension MutablePersistable {
    public func makeRequest<Fetched>(_ association: HasOneAssociation<Self, Fetched>) -> QueryInterfaceRequest<Fetched> where Fetched: TableMapping {
        return association.makeRequest(from: self)
    }
    
    public func fetchOne<Fetched>(_ db: Database, _ association: HasOneAssociation<Self, Fetched>) throws -> Fetched? where Fetched: TableMapping & RowConvertible {
        return try association.makeRequest(from: self).fetchOne(db)
    }
}