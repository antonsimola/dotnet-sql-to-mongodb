using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;
using NUnit.Framework;
using SQLToMongoDB;

namespace SQLToMongoDBTests;

public class FunctionsTests : BaseMongoTest
{
    private List<PaymentWithDate> _dbContent;


    public override void SetupData(MongoClient client)
    {
        _dbContent =
            Enumerable.Range(1, 12)
                .Select(i => new PaymentWithDate(new DateTime(2000 + i, 1, 1, 0, 0, 0, DateTimeKind.Utc), i * 1000))
                .ToList();

        var coll = client.GetDatabase("db").GetCollection<PaymentWithDate>("payments");
        coll.InsertMany(_dbContent);

        var mathColl = client.GetDatabase("db").GetCollection<dynamic>("math");

        mathColl.InsertMany(new[] { new { Value = Math.PI * 2 } });
    }

    public record FuncExpectDouble(string Func, double Expect); 
    public  record FuncExpectArray(string Func, object[] Expect);
    
    public static FuncExpectDouble[] GetDoubleCases()
    {
        var twopi = Math.PI
                    * 2;
        return new FuncExpectDouble[]
        {
            new("abs", Math.Abs(twopi)),
            new("sin", Math.Sin(twopi)),
            new("cos", Math.Cos(twopi)),
            new("tan", Math.Tan(twopi)),
            new("floor", Math.Floor(twopi)),
            new("ceil", Math.Ceiling(twopi)),
        };
    }

    [Test]
    public void DateFuncsGroupBy()
    {
        var expected = Enumerable.Range(1, 12).Select(i => i * 1000).Sum();
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(
            "select Sum(Amount) from payments GROUP BY Month(Timestamp)"
        );
        Assert.AreEqual(expected, list[0].Amount);
    }

    [Test]
    public void DateFuncsSelect()
    {
        var expected = Enumerable.Range(1, 12)
                .Select(i => new PaymentWithDate(new DateTime(2000 + i, 1, 1, 0, 0, 0, DateTimeKind.Utc), i * 1000))
                .Select(p => p.Timestamp)
                .Select(p => new
                {
                    Year = p.Year, p.Month, DayOfMonth = p.Day, p.Hour, p.Minute, p.Second, p.Millisecond, p.DayOfYear
                })
            ;
        var list = _client.GetDatabase("db").SqlQuery<dynamic>(
            @"select Year(Timestamp) as Year, Month(Timestamp) as Month, DayOfMonth(Timestamp) as DayOfMonth, Hour(Timestamp) as Hour,
  
                    Minute(Timestamp) as Minute, Second(Timestamp) as Second,
                    Millisecond(Timestamp) as Millisecond,  DayOfYear(Timestamp) as DayOfYear
            from payments"
        );


        TestUtils.AssertJsonEqual(expected, list);
    }




    [Test]
    public void FuncTests([ValueSource(nameof(GetDoubleCases))] FuncExpectDouble testCase)
    {
        var res = _client.GetDatabase("db").SqlQuery<dynamic>($"Select {testCase.Func}(Value) as Result from math");
        Assert.AreEqual(testCase.Expect, res[0].Result, 0.001);
        
    }
}