using System.Collections.Generic;
using System.Dynamic;
using Mongo2Go;
using MongoDB.Driver;
using NUnit.Framework;
using SQLToMongoDB;

namespace SQLToMongoDBTests;

using static TestUtils;

[TestFixture]
public class WhereTests
{
    private MongoDbRunner db;

    private string _connString = "mongodb://localhost:27017";

    private MongoClient _client;

    private List<User> _dbContent;

    [OneTimeSetUp]
    public void Setup()
    {
        db = Mongo2Go.MongoDbRunner.Start(singleNodeReplSet: true);
        _connString = db.ConnectionString;
        _client = new MongoClient(_connString);
        SetupData(_client);
    }

    [OneTimeTearDown]
    public void Cleanup()
    {
        db?.Dispose();
    }

    private void SetupData(MongoClient client)
    {
        var coll = client.GetDatabase("db").GetCollection<User>("users");

        _dbContent = new List<User>()
        {
            new User()
            {
                Name = "Hello", Age = 1, Salary = 1000.0, NullTest = "HasValue",
                Address = { Postal = "12345", Street = "HelloStreet" },
                Tags = new List<string>() { "Tag", "Tag2" }
            },
            new User()
            {
                Name = "World", Age = 2, Salary = 2000.0, Address = { Postal = "12345", Street = "WorldStreet", },
                Tags = new List<string>() { "Tag" }
            }
        };

        coll.InsertMany(_dbContent);
    }


    [Test]
    public void Eq()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users where Age = 1");
        AssertJsonEqual(new List<User> { _dbContent[0] }, list);
    }


    [Test]
    public void EqString()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users where Name = 'Hello'");
        AssertJsonEqual(new List<User> { _dbContent[0] }, list);
    }

    [Test]
    public void Gt()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users where Age > 1");
        AssertJsonEqual(new List<User> { _dbContent[1] }, list);
    }

    [Test]
    public void Gte()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users where Age >= 1");
        AssertJsonEqual(_dbContent, list);
    }

    [Test]
    public void Lte()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users where Age <= 2 ");
        AssertJsonEqual(_dbContent, list);
    }

    [Test]
    public void Lt()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users where Age < 2 ");
        AssertJsonEqual(new List<User>() { _dbContent[0] }, list);
    }

    [Test]
    public void NeExclamation()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users where Age != 2 ");
        AssertJsonEqual(new List<User>() { _dbContent[0] }, list);
    }

    [Test]
    public void NeDiamond()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users where Age <> 2 ");
        AssertJsonEqual(new List<User>() { _dbContent[0] }, list);
    }

    [Test]
    public void WithSelect()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select Name from users where Age <> 2 ");

        dynamic res = new ExpandoObject();
        res.Name = "Hello";
        AssertJsonEqual(new List<dynamic>() { res }, list);
    }

    [Test]
    public void Deep()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where Address.Street = 'HelloStreet' ");
        AssertJsonEqual(new List<dynamic>() { _dbContent[0] }, list);
    }

    [Test]
    public void ExpressionFlipped()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where  'HelloStreet' = Address.Street ");
        AssertJsonEqual(new List<dynamic>() { _dbContent[0] }, list);
    }

    [Test]
    public void And()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users where Name = 'Hello' and Age = 1");
        AssertJsonEqual(new List<dynamic>() { _dbContent[0] }, list);
    }

    [Test]
    public void And2()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users where Age = 2 and Age = 1");
        AssertJsonEqual(new List<dynamic>(), list);
    }

    [Test]
    public void Or()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select * from users where Name = 'Hello' or Age = 2");
        AssertJsonEqual(_dbContent, list);
    }

    [Test]
    public void AndOr()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where Name = 'Hello' and (Age = 1 or Age = 2)");
        AssertJsonEqual(new List<User>() { _dbContent[0] }, list);
    }

    [Test]
    public void AndOr2()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where (Name = 'Hello' and Age = 1) or Age = 2");
        AssertJsonEqual(_dbContent, list);
    }

    [Test]
    public void AndOr3()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where (Name = 'Hello' or (Age = 1 or Age = 2)) or Age = 3");
        AssertJsonEqual(_dbContent, list);
    }

    [Test]
    public void AndOr4()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where (Name = 'Hello' or (Age = 1 and Age = 2)) or Age = 3");
        AssertJsonEqual(new List<User>() { _dbContent[0] }, list);
    }

    [Test]
    public void AndOr5()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where (Name = 'Hello' or (Age = 1 and Age = 2)) or Age = 3");
        AssertJsonEqual(new List<User>() { _dbContent[0] }, list);
    }


    [Test]
    public void Double()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where Salary > 1000.0");
        AssertJsonEqual(new List<User>() { _dbContent[1] }, list);
    }

    [Test]
    public void NullEq()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where NullTest = NULL");
        AssertJsonEqual(new List<User>() { _dbContent[1] }, list);
    }

    [Test]
    public void NullIs()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where NullTest IS NULL");
        AssertJsonEqual(new List<User>() { _dbContent[1] }, list);
    }

    [Test]
    public void NotNullEq()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where NullTest != NULL");
        AssertJsonEqual(new List<User>() { _dbContent[0] }, list);
    }

    [Test]
    public void NotNullIs()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where NullTest IS NOT NULL");
        AssertJsonEqual(new List<User>() { _dbContent[0] }, list);
    }

    [Test]
    public void ExprArithmetic()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where Age = 2 * 1 + 1 - 1 / 1"); // = 2
        AssertJsonEqual(new List<User>() { _dbContent[1] }, list);
    }
    
    [Test]
    public void ExprSum()
    {
        var list = _client.GetDatabase("db")
            .SqlQuery<dynamic>(@"select * from users where Age = 1 + 1");
        AssertJsonEqual(new List<User>() { _dbContent[1] }, list);
    }
}