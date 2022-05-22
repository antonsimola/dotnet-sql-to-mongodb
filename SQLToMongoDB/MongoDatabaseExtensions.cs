using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class SqlParseException : Exception
{
    public SqlParseException(string message) : base(message)
    {
    }
}

public static class MongoDatabaseExtensions
{
    public static IList<T> SqlQuery<T>(this IMongoClient client, string query)
    {
        //Todo should be able to query client level 
        throw new NotImplementedException();
    }

    public static IList<T> SqlQuery<T>(this IMongoCollection<T> coll, string query)
    {
        //Todo should be able to query collection level (from clause not needed?) 
        throw new NotImplementedException();
    }


    public static IList<T> SqlQuery<T>(this IMongoDatabase db, string query, MongoSqlQueryOptions? options = null)
    {

        options ??= new MongoSqlQueryOptions() { IgnoreIdByDefault = true };
        var parser = new TSql150Parser(initialQuotedIdentifiers: false);

        using var stringReader = new StringReader(query);

        var tree = parser.Parse(stringReader, out IList<ParseError>? errors);

        if (errors?.Count > 0)
        {
            throw new SqlParseException(GetErrorStrings(query, errors));
        }

        var mongoQueryBuilderVisitor = new MongoQueryBuilderVisitor<T>();

        tree.Accept(mongoQueryBuilderVisitor);

        var parts = mongoQueryBuilderVisitor.QueryParts;

        var coll = db.GetCollection<T>(parts.CollectionName);

        var agg = coll.Aggregate();

        agg = AppendJoin<T>(agg, parts);

        agg = AppendMatch<T>(agg, parts);
        agg = AppendGroupBy(agg, parts);
        agg = AppendSort(agg, parts);
        agg = AppendProject(agg, parts, options);
        agg = AppendSkip(agg, parts);
        agg = AppendLimit(agg, parts);
        Console.WriteLine(agg);
        return agg.ToList();
    }

    private static IAggregateFluent<T> AppendJoin<T>(IAggregateFluent<T> agg, QueryParts<T> parts)
    {
        if (parts.JoinDefinition == null)
        {
            return agg;
        }

        return agg.Lookup(parts.JoinDefinition.FromTable, parts.JoinDefinition.LocalField,
            parts.JoinDefinition.ForeignField, parts.JoinDefinition.AsField).As<T>();
    }

    private static IAggregateFluent<T> AppendSort<T>(IAggregateFluent<T> agg, QueryParts<T> parts)
    {
        if (parts.OrderByDefinition == null)
        {
            return agg;
        }

        return agg.Sort(parts.OrderByDefinition);
    }

    private static IAggregateFluent<T> AppendGroupBy<T>(IAggregateFluent<T> agg, QueryParts<T> parts)
    {
        if (parts.GroupByDefinition == null) return agg;
        return agg.Group<T>(parts.GroupByDefinition);
    }

    private static string GetErrorStrings(string fullQuery, IList<ParseError> errors)
    {
        var fullErrorString = "";
        foreach (var e in errors)
        {
            fullErrorString += $"Line {e.Line}, Column {e.Column} : {e.Message}\n";
            var lineString = fullQuery.Split("\n").Skip(e.Line - 1).Take(1).ToArray()[0];
            fullErrorString += lineString + "\n";
            fullErrorString += new String(' ', e.Column - 1) + "^\n";
        }

        return fullErrorString.Trim();
    }

    private static IAggregateFluent<T> AppendProject<T>(IAggregateFluent<T> agg, QueryParts<T> parts,
        MongoSqlQueryOptions options)
    {
        if (parts.ProjectionDefinition is null)
        {
            if (options.IgnoreIdByDefault)
            {
                return agg.Project<T>(new BsonDocument("_id", 0));
            }

            return agg;

        }

        return agg.Project<T>(parts.ProjectionDefinition);
    }

    private static IAggregateFluent<T> AppendMatch<T>(IAggregateFluent<T> agg, QueryParts<T> parts)
    {
        if (parts.FilterDefinition is null)
        {
            return agg;
        }

        return agg.Match(parts.FilterDefinition);
    }

    private static IAggregateFluent<T> AppendLimit<T>(IAggregateFluent<T> agg, QueryParts<T> parts)
    {
        if (parts.Limit is null)
        {
            return agg;
        }

        return agg.Limit(parts.Limit.Value);
    }

    private static IAggregateFluent<T> AppendSkip<T>(IAggregateFluent<T> agg, QueryParts<T> parts)
    {
        if (parts.Skip is null)
        {
            return agg;
        }

        return agg.Skip(parts.Skip.Value);
    }
}

public class MongoSqlQueryOptions
{
    public bool IgnoreIdByDefault { get; set; }
}