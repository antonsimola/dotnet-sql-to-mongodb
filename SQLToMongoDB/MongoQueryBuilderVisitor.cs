using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class MongoQueryBuilderVisitor<T> : TSqlFragmentVisitor
{
    public QueryParts<T> QueryParts { get; }
    public MongoQueryBuilderVisitor()
    {
        QueryParts = new QueryParts<T>();
    }
    

    public override void Visit(SelectStatement statement)
    {
        Console.WriteLine(statement.ToString());

        if (statement.QueryExpression is QuerySpecification spec)
        {
            QueryParts.CollectionName = new FromCollectionVisitor().Visit(spec);
            QueryParts.ProjectionDefinition = new ProjectionBuilderVisitor<T>().Visit(spec);
            QueryParts.FilterDefinition = new FilterBuilderVisitor<T>().Visit(spec);
        }
        else
        {
            throw new NotImplementedException("unknown Query Expression " + statement.QueryExpression.GetType());
        }
    }

   
}