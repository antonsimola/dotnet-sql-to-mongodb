using System.Collections.Immutable;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class QueryParts<T>
{
    public List<BsonElement> Fields { get; set; } = new List<BsonElement>();

    public FilterDefinition<T>? FilterDefinition;

    public ProjectionDefinition<T>? ProjectionDefinition { get; set; }

    public string CollectionName { get; set; }
}