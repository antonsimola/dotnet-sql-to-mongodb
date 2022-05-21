using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;
using NUnit.Framework;
using SQLToMongoDB;
using static SQLToMongoDBTests.TestUtils;

namespace SQLToMongoDBTests;

[TestFixture]
public class OrderByTests: BaseMongoTest
{
    private List<User> _dbContent;

    public override void SetupData(MongoClient client)
    {
        _dbContent = new List<User>()
        {
            new User()
            {
                Name = "U1", Age = 1, Salary = 1000.0, NullTest = "HasValue",
                DateOfBirth = new DateTime(2000,1,1,0,0,0,DateTimeKind.Utc),
                Address = { Postal = "12345", Street = "HelloStreet" },
                Tags = new List<string>() { "Tag", "Tag2" }
            },
            new User()
            {
                DateOfBirth = new DateTime(2010,1,1,0,0,0,DateTimeKind.Utc),
                Name = "U2", Age = 3, Salary = 2000.0, Address = { Postal = "12345", Street = "WorldStreet", },
                Tags = new List<string>() { "Tag" }
            },
            new User()
            {
                DateOfBirth = new DateTime(2010,1,1,0,0,0,DateTimeKind.Utc),
                Name = "U3", Age = 2, Salary = 2000.0, Address = { Postal = "12345", Street = "WorldStreet", },
                Tags = new List<string>() { "Tag" }
            },
            
            new User()
            {
                DateOfBirth = new DateTime(2010,1,1,0,0,0,DateTimeKind.Utc),
                Name = "u3", Age = 2, Salary = 2000.0, Address = { Postal = "12345", Street = "WorldStreet", },
                Tags = new List<string>() { "Tag" }
            },
   
        };
        var coll = client.GetDatabase("db").GetCollection<User>("users");


        coll.InsertMany(_dbContent);
    }

    [Test]
    public void SimpleOrderBy()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users order by Age");
        AssertJsonEqual(_dbContent.OrderBy(i => i.Age), list);
    }
    
    [Test]
    public void SimpleOrderByDesc()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users order by Age desc");
        AssertJsonEqual(_dbContent.OrderByDescending(i => i.Age), list);
    }
    
    [Test]
    public void SimpleOrderByName()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users order by Name");
        AssertJsonEqual(_dbContent.OrderBy(i => i.Name, StringComparer.Ordinal), list);
    }
    
    [Test]
    public void SimpleOrderByMultiple()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users order by Age, Name desc");
        AssertJsonEqual(_dbContent.OrderBy(i => i.Age).ThenByDescending(i => i.Name, StringComparer.Ordinal), list);
    }
    [Test]
    public void SimpleOrderByDeepDesc()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users order by Address.Street desc");
        AssertJsonEqual(_dbContent.OrderByDescending(i => i.Address.Street, StringComparer.Ordinal), list);
    }
}