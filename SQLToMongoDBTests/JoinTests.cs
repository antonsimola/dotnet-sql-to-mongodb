using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;
using Newtonsoft.Json;
using NUnit.Framework;
using SQLToMongoDB;

namespace SQLToMongoDBTests;

public class JoinTests : BaseMongoTest
{
    private List<Order> _orderContent;
    private List<Item> _itemContent;
    private List<OrderWithItems> expectedResult;

    public override void SetupData(MongoClient client)
    {
        var orderColl = client.GetDatabase("db").GetCollection<Order>("orders");

        _orderContent = new List<Order>()
        {
            new Order("1", new DateTime(2010, 1, 1,0,0,0,DateTimeKind.Utc)),
            new Order("2", new DateTime(2010, 1, 1,0,0,0,DateTimeKind.Utc)),
        };
        orderColl.InsertMany(_orderContent);

        var itemsColl = client.GetDatabase("db").GetCollection<Item>("items");

        _itemContent = new List<Item>()
        {
            new Item("1","1", "Food", 1),
            new Item("2","1", "Drink", 2),
            new Item("3","1", "Ball", 1),
            new Item("4","2", "Pizza", 1),
            new Item("5","2", "Hamburger", 1),
        };
        itemsColl.InsertMany(_itemContent);

        expectedResult = new List<OrderWithItems>();
        
        foreach (var order in _orderContent)
        {

            expectedResult.Add(new OrderWithItems(order.Id, order.OrderTime,
                _itemContent.Where(i => i.OrderId == order.Id).ToList()));

        }
    }

    [Test]
    public void SimpleJoin()
    {
        var a = _client.GetDatabase("db").SqlQuery<dynamic>("Select * from orders");
        var b = _client.GetDatabase("db").SqlQuery<dynamic>("Select * from items");
        

        Console.WriteLine(JsonConvert.SerializeObject(a));
        Console.WriteLine(JsonConvert.SerializeObject(b));
        
        
        var list = _client.GetDatabase("db")
            .SqlQuery<OrderWithItems>("SELECT * from orders LEFT JOIN items as Items ON orders._id = Items.OrderId", new MongoSqlQueryOptions(){IgnoreIdByDefault = false} );
        
        TestUtils.AssertJsonEqual(expectedResult, list);
    }
}