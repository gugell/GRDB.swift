extension MutablePersistable {
    public func fetchOne<Fetched>(_ db: Database, _ association: HasOneAssociation<Self, Fetched>) throws -> Fetched? where Fetched: TableMapping & RowConvertible {
        return try association.belonging(to: self).fetchOne(db)
    }
}
