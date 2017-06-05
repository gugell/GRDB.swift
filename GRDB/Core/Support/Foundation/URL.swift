import Foundation

/// NSURL stores its absoluteString in the database.
extension NSURL : DatabaseValueConvertible {
    
    /// Returns a value that can be stored in the database.
    /// (the URL's absoluteString).
    public var databaseValue: DatabaseValue {
        return absoluteString?.databaseValue ?? .null
    }
    
    /// Returns an NSURL initialized from *dbValue*, if possible.
    public static func fromDatabaseValue(_ dbValue: DatabaseValue) -> Self? {
        guard let string = String.fromDatabaseValue(dbValue) else {
            return nil
        }
        return cast(URL(string: string))
    }
}

/// URL stores its absoluteString in the database.
extension URL : DatabaseValueConvertible {
    /// Returns a value initialized from *databaseValue*, if possible.
    public static func fromDatabaseValue(_ databaseValue: DatabaseValue) -> URL? {
        return NSURL.fromDatabaseValue(databaseValue).flatMap { $0 as URL }
    }
}
