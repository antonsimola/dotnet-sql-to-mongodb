using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class OrderByBuilderVisitor<T>: TSqlConcreteFragmentVisitor
{
    public new SortDefinition<T> Visit(QuerySpecification spec)
    {
        if (spec.OrderByClause == null) return null;

        var groupByFields = new List<BsonElement>();
        var reverseMap = new Dictionary<object, string>();

        var i = 0;

        foreach (var orderByField in spec.OrderByClause.OrderByElements)
        {
            var expressionVisitor = new ExpressionVisitor<T>();
            orderByField.Accept(expressionVisitor);
            var fieldPart = expressionVisitor.GetColumnName();
            var direction = orderByField.SortOrder == SortOrder.Descending ? -1 : 1;
            groupByFields.Add(new BsonElement(fieldPart,direction));
        }

        return (SortDefinition<T>) Builders<T>.Sort.Combine(new BsonDocument(groupByFields));
    }
    
}