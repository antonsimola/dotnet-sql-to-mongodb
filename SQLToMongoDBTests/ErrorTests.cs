using MongoDB.Driver;
using NUnit.Framework;
using SQLToMongoDB;

namespace SQLToMongoDBTests;

[TestFixture]
public class ErrorTests
{

    [Test]
    public void ItReportsErrors()
    {

        Assert.Throws<SqlParseException>(() => MongoDatabaseExtensions.SqlQuery<User>((IMongoDatabase)null, "Select * from users where ((())"));
    }
}