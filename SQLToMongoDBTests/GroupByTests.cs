using System;
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


    public record Case(string Query, object Expected, Func<dynamic,dynamic> orderBy = null );

    public static Case[] GetCases()
    {
        return new Case[]
        {
            new(@"SELECT SUM(Age) FROM users GROUP BY GroupByTest", new[] { new { Age = 3 } }),

            new(@"SELECT Max(Age) FROM users GROUP BY GroupByTest", new[] { new { Age = 2 } }),

            new(@"SELECT MIN(Age) FROM users GROUP BY GroupByTest", new[] { new { Age = 1 } }),

            new(@"SELECT AVG(Age) FROM users GROUP BY GroupByTest", new[] { new { Age = 1.5 }, }),

            new(@"SELECT SUM(Age) as SumAge FROM users GROUP BY GroupByTest", new[] { new { SumAge = 3 }, }),

            new(
                @"SELECT Sum(Age) as sum, Min(Age) as min, max(Age) as max, AVG(Age) as avg  FROM users GROUP BY GroupByTest",
                new[]
                {
                    new { sum = 3, min = 1, max = 2, avg = 1.5 },
                }),

            new(@"SELECT Sum(Amount) as SumAmount, Year, Month  FROM payments GROUP BY Year, Month Order by Month",
                new[]
                {
                    new { SumAmount = 3000.0, Year = "2020", Month = "Feb" },
                    new { SumAmount = 3000.0, Year = "2020", Month = "Jan" },
                }, d => d.Month),

            new(@"SELECT Avg(Age) as AvgAge, MIN(Address.Postal) as Postal FROM users GROUP BY Address.Postal",
                new[] { new { AvgAge = 1.5, Postal = "12345" } }),

            new(@"SELECT Avg(Age) as AvgAge, Address.Postal as Postal FROM users GROUP BY Address.Postal", new[]
            {
                new { AvgAge = 1.5, Postal = "12345" }
            }),

            new(@"SELECT Avg(Age), Name, Address.Street FROM users GROUP BY Name, Address.Street", new[]
            {
                new { Age = 1.0, Name = "Hello", Address = new { Street = "HelloStreet" } },
                new { Age = 2.0, Name = "World", Address = new { Street = "WorldStreet" } }
            }),


            new(@"SELECT Count(*) FROM payments GROUP BY NULL", new[] { new { Count = 3 } })
        };
    }

    [Test]
    public void TestGroupBy([ValueSource(nameof(GetCases))] Case testCase)
    {
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(testCase.Query);
        if (testCase.orderBy != null)
        {
            list = list.OrderBy(testCase.orderBy).ToList();
            
        }
        AssertJsonEqual(testCase.Expected, list);
    }
}