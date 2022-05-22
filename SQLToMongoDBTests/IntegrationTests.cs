using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Driver;
using NUnit.Framework;
using SQLToMongoDB;

namespace SQLToMongoDBTests;

public class IntegrationTests : BaseMongoTest
{
    private List<User> _dbContent;

    public override void SetupData(MongoClient client)
    {
        var coll = client.GetDatabase("db").GetCollection<User>("users");

        _dbContent = new List<User>()
        {
            new User() { Name = "Hello", Age = 1, Address = { Postal = "12345", Street = "HelloStreet" } },
            new User() { Name = "World", Age = 2, Address = { Postal = "12345", Street = "WorldStreet" } },
            new User() { Name = "Moi", Age = 3, Address = { Postal = "45678", Street = "BlaaStreet" } },
            new User() { Name = "Maailma", Age = 4, Address = { Postal = "45678", Street = "BlaaStreet" } }
        };
        coll.InsertMany(_dbContent);
    }

    [Test]
    public void AvgAgeOfUsersLivingInBlaaStreet()
    {
        var sql =
            "SELECT AVG(Age) as AvgAge from users where [Address.Street] = 'BlaaStreet' GROUP BY [Address.Street] ";
        var db = _client.GetDatabase("db");
        var res = db.SqlQuery<dynamic>(sql);
        Assert.AreEqual(3.5, res[0].AvgAge);
    }
    
    [Test]
    public void WhereGroupByOrderBy()
    {
        
        ///TODO we need to look into order by, and also include aggregation functions from there to the group by 
        var sql =
            "SELECT [Address.Postal] as Postal, Sum(Age) as SumAge from users where Age > 1 GROUP BY [Address.Postal] Order By Count(_id) DESC";
        var db = _client.GetDatabase("db");
        var res = db.SqlQuery<dynamic>(sql);
        Assert.AreEqual(3.5, res[0].SumAge);
    }


    [Test]
    public void SimpleWhere()
    {
        var sql = "SELECT * from users where Age > 2 AND [Address.Postal] = '45678'";
        var db = _client.GetDatabase("db");
        var res = db.SqlQuery<User>(sql);
        Assert.AreEqual(2, res.Count);
        TestUtils.AssertJsonEqual(_dbContent.Skip(2).ToList(), res);
    }
}