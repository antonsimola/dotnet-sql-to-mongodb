using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class ProjectionBuilderVisitor<T> : TSqlConcreteFragmentVisitor
{
    public bool ExcludeIdByDefault { get; set; } = true;

    public new ProjectionDefinition<T>? Visit(QuerySpecification spec)
    {
        var fields = new List<BsonElement>();

        var hasGroupBy = spec.GroupByClause != null;
        
        if (!hasGroupBy)
        {
            foreach (var select in spec.SelectElements)
            {
                if (select is SelectStarExpression)
                {
                    return null;
                }

                var visitor = new ExpressionVisitor<T>();

                select.AcceptChildren(visitor);

                var newName = visitor.GetColumnNewName();

                //fields.Add(new BsonElement( newName, GetFieldExpression(hasGroupBy ? newName : fieldName)));
                fields.Add(new BsonElement(newName, visitor.GetAsBsonValue()));
            }
        }
        


        if (ExcludeIdByDefault)
        {
            fields.Add(new BsonElement("_id", 0));
        }

        return Builders<T>.Projection.Combine(new BsonDocument(fields));
    }


    private string GetFieldExpression(string path)
    {
        return $"${path}";
    }
}