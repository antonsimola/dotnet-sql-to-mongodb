using System.Collections.Immutable;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class QueryParts<T>
{
    public FilterDefinition<T>? FilterDefinition;
    public ProjectionDefinition<T>? ProjectionDefinition { get; set; }
    public ProjectionDefinition<T>? GroupByDefinition { get; set; }

    public string CollectionName { get; set; }
    public int? Limit { get; set; }
    public int? Skip { get; set; }
    public SortDefinition<T>? OrderByDefinition { get; set; }
    public JoinDefinition? JoinDefinition { get; set; }
}