using System;
using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json;

namespace SQLToMongoDBTests;

[BsonIgnoreExtraElements(true)]
public record User
{
    public string Name { get; set; }
    public int Age { get; set; }
    public Address Address { get; set; } = new Address();

    public IList<User> Friends { get; set; } = new List<User>();

    public IList<string> Tags { get; set; } = new List<string>();
    public double Salary { get; set; }
    
    public string NullTest { get; set; }
    public string GroupByTest { get; set; }
    public string Year { get; set; }
    public DateTime? DateOfBirth { get; set; }
}

public record Address
{
    public string Street { get; set; }
    public string Postal { get; set; }
}

public record Payment(string Year, string Month, double Amount);