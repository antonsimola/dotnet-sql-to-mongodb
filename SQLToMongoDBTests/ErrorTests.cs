using NUnit.Framework;
using SQLToMongoDB;

namespace SQLToMongoDBTests;

[TestFixture]
public class ErrorTests
{

    [Test]
    public void ItReportsErrors()
    {

        Assert.Throws<SqlParseException>(() => MongoDatabaseExtensions.SqlQuery<User>(null, "Select * from users where ((())"));
    }
}