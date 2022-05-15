using System.Collections.Generic;
using System.Linq;
using Mongo2Go;
using MongoDB.Driver;
using NUnit.Framework;
using SQLToMongoDB;

namespace SQLToMongoDBTests;

public class LimitSkipTests : BaseMongoTest
{
    private List<User> _dbContent;

    public override void SetupData(MongoClient client)
    {
        var coll = client.GetDatabase("db").GetCollection<User>("users");

        _dbContent = new List<User>()
        {
            new() { Name = "Hello", Age = 1, },
            new() { Name = "World", Age = 2, },
            new() { Name = "!", Age = 3, }
        };

        coll.InsertMany(_dbContent);
    }

    [Test]
    public void Top()
    {
        var list = _client.GetDatabase("db").SqlQuery<User>("SELECT TOP 1 * from users");
        TestUtils.AssertJsonEqual(_dbContent.Take(1).ToList(), list);
    }
    
    [Test]
    public void Offset()
    {
        var list = _client.GetDatabase("db").SqlQuery<User>("SELECT * from users ORDER BY Age OFFSET 1 ROWS");
        TestUtils.AssertJsonEqual(_dbContent.Skip(1).ToList(), list);
    }
    
    [Test]
    public void TopOffset()
    {
        var list = _client.GetDatabase("db").SqlQuery<User>("SELECT TOP 1 * from users ORDER BY Age OFFSET 1 ROWS");
        TestUtils.AssertJsonEqual(_dbContent.Skip(1).Take(1).ToList(), list);
    }
    
    [Test]
    public void OffsetAndFetch()
    {
        var list = _client.GetDatabase("db").SqlQuery<User>("SELECT * from users ORDER BY Age OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY");
        TestUtils.AssertJsonEqual(_dbContent.Skip(1).ToList(), list);
    }
}

public abstract class BaseMongoTest
{
    public MongoDbRunner db;

    public string _connString = "mongodb://localhost:27017";

    public MongoClient _client;


    [OneTimeSetUp]
    public void Setup()
    {
        db = Mongo2Go.MongoDbRunner.Start(singleNodeReplSet: true);
        _connString = db.ConnectionString;
        _client = new MongoClient(_connString);
        SetupData(_client);
    }

    public abstract void SetupData(MongoClient client);

    [OneTimeTearDown]
    public void Cleanup()
    {
        db?.Dispose();
    }
}