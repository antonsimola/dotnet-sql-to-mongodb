using System;
using System.Collections.Generic;
using System.Linq;
using Mongo2Go;
using MongoDB.Driver;
using NUnit.Framework;
using SQLToMongoDB;

namespace SQLToMongoDBTests;

public class SelectTests
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

    private void SetupData(MongoClient client)
    {
        var coll = client.GetDatabase("db").GetCollection<User>("users");

        _dbContent = new List<User>()
        {
            new User()
            {
                Name = "Hello", Age = 1, Address = { Postal = "12345", Street = "HelloStreet" },
                Tags = new List<string>() { "Tag", "Tag2" }
            },
            new User()
            {
                Name = "World", Age = 2, Address = { Postal = "12345", Street = "WorldStreet", },
                Tags = new List<string>() { "Tag" }
            }
        };

        coll.InsertMany(_dbContent);
    }


    [Test]
    public void Star()
    {
        var actual = _client.GetDatabase("db").SqlQuery<User>("select * from users");
        TestUtils.AssertJsonEqual(_dbContent, actual);
    }

    [Test]
    public void AllFields()
    {
        TestUtils.AssertJsonEqual(_dbContent,
            _client.GetDatabase("db").SqlQuery<User>("select Name, Age, Address, Friends,Tags from users"));
    }

    [Test]
    public void Fields()
    {
        var coupleFields = _dbContent.Select(d =>
                d with { Address = new Address() { Postal = null, Street = null }, Tags = new List<string>() })
            .ToList();

        TestUtils.AssertJsonEqual(coupleFields, _client.GetDatabase("db").SqlQuery<User>("select Name, Age from users"));
    }


    [Test]
    public void Field()
    {
        var noAge = _dbContent
            .Select(d => d with
            {
                Age = default, Address = new Address() { Postal = null, Street = null }, Tags = new List<string>()
            }).ToList();
        TestUtils.AssertJsonEqual(noAge, _client.GetDatabase("db").SqlQuery<User>("select Name from users"));
    }

    [Test]
    public void AsFields()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>("select Name as __Name, Age as __Age from users");
        Assert.AreEqual(list.Select(s => s.__Age).ToList(), new List<int>() { 1, 2 });
        Assert.AreEqual(list.Select(s => s.__Name).ToList(), new List<string>() { "Hello", "World" });
    }


    [Test]
    public void Deep()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select [Address.Street] from users");

        Assert.AreEqual(true, !((Type)list[0].GetType()).GetProperties().Any(p => p.Name == "Name"));
        Assert.AreEqual(list.Select(s => s.Address.Street).ToList(),
            new List<string>() { "HelloStreet", "WorldStreet" });
    }


    [Test]
    public void DeepAs()
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select [Address.Street] as street from users");
        TestUtils.AssertJsonEqual(new[]
        {
            new { street = "HelloStreet" },
            new { street = "WorldStreet" }
        }, list);
    }

     [Test]
     public void ArrayElement()
     {
         var list = _client.GetDatabase("db").SqlQuery<dynamic>(@"select ArrayElemAt(Tags, 0) as Tag from users");
         TestUtils.AssertJsonEqual(new[]
         {
         new { Tag = "Tag" },
         new { Tag = "Tag" }
         }, list);
     }

  


    [OneTimeTearDown]
    public void Cleanup()
    {
        db?.Dispose();
    }
}