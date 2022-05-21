using System.Collections.Generic;
using System.Linq;
using Mongo2Go;
using MongoDB.Driver;
using NUnit.Framework;
using SQLToMongoDB;
using static SQLToMongoDBTests.TestUtils;

namespace SQLToMongoDBTests;

public class GroupByTests : BaseMongoTest
{
    List<User> _dbContent;

    List<Payment> _dbContentPayment;

    public override void SetupData(MongoClient client)
    {
        var coll = client.GetDatabase("db").GetCollection<User>("users");

        _dbContent = new List<User>()
        {
            new User()
            {
                Name = "Hello", Age = 1, Address = { Postal = "12345", Street = "HelloStreet" },
                GroupByTest = "1",
                Tags = new List<string>() { "Tag", "Tag2" }
            },
            new User()
            {
                Name = "World", Age = 2, Address = { Postal = "12345", Street = "WorldStreet", },
                GroupByTest = "1",
                Tags = new List<string>() { "Tag" }
            }
        };

        coll.InsertMany(_dbContent);

        var paymentColl = client.GetDatabase("db").GetCollection<Payment>("payments");

        _dbContentPayment = new List<Payment>()
        {
            new Payment("2020", "Jan", 1000),
            new Payment("2020", "Jan", 2000),
            new Payment("2020", "Feb", 3000),
        };

        paymentColl.InsertMany(_dbContentPayment);
    }


    [Test]
    public void SimpleSum()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"SELECT SUM(Age) FROM users GROUP BY GroupByTest");
        AssertJsonEqual(new[]
        {
            new { Age = 3 },
        }, list);
    }

    [Test]
    public void SimpleMax()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"SELECT Max(Age) FROM users GROUP BY GroupByTest");
        AssertJsonEqual(new[]
        {
            new { Age = 2 },
        }, list);
    }

    [Test]
    public void SimpleMin()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"SELECT MIN(Age) FROM users GROUP BY GroupByTest");
        AssertJsonEqual(new[]
        {
            new { Age = 1 },
        }, list);
    }

    [Test]
    public void SimpleAvg()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"SELECT AVG(Age) FROM users GROUP BY GroupByTest");
        AssertJsonEqual(new[]
        {
            new { Age = 1.5 },
        }, list);
    }


    [Test]
    public void SimpleSumAs()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"SELECT SUM(Age) as SumAge FROM users GROUP BY GroupByTest");
        AssertJsonEqual(new[]
        {
            new { SumAge = 3 },
        }, list);
    }

    [Test]
    public void MultipleAggs()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(
                @"SELECT Sum(Age) as sum, Min(Age) as min, max(Age) as max, AVG(Age) as avg  FROM users GROUP BY GroupByTest");
        AssertJsonEqual(new[]
        {
            new { sum = 3, min = 1, max = 2, avg = 1.5 },
        }, list);
    }

    [Test]
    public void MultiGroup()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(
                @"SELECT Sum(Amount) as SumAmount, MIN(Year), Min(Month)  FROM payments GROUP BY Year, Month");
        AssertJsonEqual(new[]
        {
            new { SumAmount = 3000.0, Year = "2020", Month = "Jan" },
            new { SumAmount = 3000.0, Year = "2020", Month = "Feb" },
        }, list.OrderByDescending(s => s.Month)); // return in random order
    }

    [Test]
    public void GroupDeep()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(
                @"SELECT Avg(Age) as AvgAge, MIN(Address.Postal) as Postal FROM users GROUP BY Address.Postal");
        AssertJsonEqual(new[]
        {
            new { AvgAge = 1.5, Postal = "12345" }
        }, list);
    }

    [Test]
    public void GroupSelectKey()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(
                @"SELECT Avg(Age) as AvgAge, Address.Postal as Postal FROM users GROUP BY Address.Postal");
        AssertJsonEqual(new[]
        {
            new { AvgAge = 1.5, Postal = "12345" }
        }, list);
    }

    [Test]
    public void GroupSelectKeyDeep()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(
                @"SELECT Avg(Age), Name, Address.Street FROM users GROUP BY Name, Address.Street");
        AssertJsonEqual(new[]
        {
            new { Age = 1.0, Name = "Hello", Address = new { Street = "HelloStreet" } },
            new { Age = 2.0, Name = "World", Address = new { Street = "WorldStreet" } }
        }, list.OrderBy(i => i.Age));
    }
    
    [Test]
    public void GroupByCounting()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(
                @"SELECT Count(*) FROM payments GROUP BY NULL");
        AssertJsonEqual(new[]
        {
            new { Count = 3}
        }, list);
    }
}