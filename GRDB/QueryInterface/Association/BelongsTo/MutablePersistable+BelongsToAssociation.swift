extension MutablePersistable {
    public func fetchOne<Fetched>(_ db: Database, _ association: BelongsToAssociation<Self, Fetched>) throws -> Fetched? where Fetched: TableMapping & RowConvertible {
        return try association.owning(self).fetchOne(db)
    }
}
