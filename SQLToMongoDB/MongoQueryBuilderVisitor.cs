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
        if (statement.QueryExpression is QuerySpecification spec)
        {
            var join = new JoinVisitor<T>().Visit(spec);

            if (join == null)
            {
                QueryParts.CollectionName = new FromCollectionVisitor().Visit(spec);
            }
            else
            {
                QueryParts.CollectionName = join.LocalTable;
                QueryParts.JoinDefinition = join;
            }
            QueryParts.ProjectionDefinition = new ProjectionBuilderVisitor<T>().Visit(spec);
            QueryParts.FilterDefinition = new FilterBuilderVisitor<T>().Visit(spec);
            QueryParts.GroupByDefinition = new GroupByBuilderVisitor<T>().Visit(spec);
            QueryParts.OrderByDefinition = new OrderByBuilderVisitor<T>().Visit(spec);
            var (skip,limit) = new SkipLimitVisitor<T>().Visit(spec);
            QueryParts.Limit = limit;
            QueryParts.Skip = skip;
        }
        else
        {
            throw new NotImplementedException("unknown statement " + statement.QueryExpression.GetType());
        }
    }

   
}