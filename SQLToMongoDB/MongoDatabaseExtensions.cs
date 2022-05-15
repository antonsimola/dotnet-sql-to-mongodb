using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class SqlParseException: Exception
{
    public SqlParseException(string message): base(message)
    {
    }
}

public static class MongoDatabaseExtensions
{
    public static IList<T> SqlQuery<T>(this IMongoDatabase db, string query)
    {
        var parser = new TSql150Parser(initialQuotedIdentifiers: false);

        using var stringReader = new StringReader(query);

        var tree = parser.Parse(stringReader, out IList<ParseError>? errors);

        if (errors?.Count > 0)
        {
            throw new SqlParseException(GetErrorStrings(query,errors));
        }

        var mongoQueryBuilderVisitor = new MongoQueryBuilderVisitor<T>();

        tree.Accept(mongoQueryBuilderVisitor);

        var parts = mongoQueryBuilderVisitor.QueryParts;

        var coll = db.GetCollection<T>(parts.CollectionName);

        var agg = coll.Aggregate();

        agg = AppendMatch<T>(agg, parts);
        var withProject = AppendProject(agg, parts);
        Console.WriteLine(withProject.ToString());
        return withProject.ToList();
    }
 
    private static string GetErrorStrings(string fullQuery, IList<ParseError> errors)
    {
        var fullErrorString = "";
        foreach (var e in errors)
        {
            fullErrorString += $"Line {e.Line}, Column {e.Column} : {e.Message}\n";
            var lineString = fullQuery.Split("\n").Skip(e.Line - 1).Take(1).ToArray()[0];
            fullErrorString += lineString + "\n";
            fullErrorString += new String(' ', e.Column-1) + "^\n";
        }
        return fullErrorString.Trim();
    }

    private static IAggregateFluent<T> AppendProject<T>(IAggregateFluent<T> agg, QueryParts<T> parts)
    {
        if (parts.ProjectionDefinition is null)
        {
            return agg.Project<T>(new BsonDocument("_id", 0));
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
}