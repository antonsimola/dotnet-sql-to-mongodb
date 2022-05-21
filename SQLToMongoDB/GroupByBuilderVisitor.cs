using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class GroupByBuilderVisitor<T> : TSqlConcreteFragmentVisitor
{
    private static object NULL = new();
    
    public new ProjectionDefinition<T> Visit(QuerySpecification spec)
    {
        if (spec.GroupByClause == null) return null;

        var groupByFields = new List<BsonElement>();
        var reverseMap = new Dictionary<object, string>();
        
        var i = 0;
        
        foreach (var grouping in spec.GroupByClause.GroupingSpecifications)
        {
            var expressionVisitor = new ExpressionVisitor<T>();
            grouping.Accept(expressionVisitor);
            var groupPart = expressionVisitor.GetAsObject();

            var f = $"g{i++}";
            groupByFields.Add(new BsonElement(f, BsonValue.Create(groupPart)));
            
            reverseMap[groupPart ?? NULL] = f;
        }

        var accumulators = new List<BsonElement>();

        foreach (var select in spec.SelectElements)
        {
            
            var expressionVisitor = new ExpressionVisitor<T>();
            select.AcceptChildren(expressionVisitor);

            var field = expressionVisitor.GetColumnNewName();
            var currentAcc = expressionVisitor.GetAsObject();

            if (reverseMap.ContainsKey(currentAcc ?? NULL))
            {
                
                // then it is not accumulator, but user wanting to select part of the group by key
                // SELECT Avg(Age), Name from user group by Name;
                //                    ^ 
                currentAcc = new BsonDocument("$first",  BsonValue.Create(currentAcc));

            }
            
            accumulators.Add(new BsonElement(field, BsonValue.Create(currentAcc)));
            
        }

        BsonDocument doc = new BsonDocument()
        {
            { "_id", new BsonDocument(groupByFields) }
        };

        doc.AddRange(accumulators);
        


        return (ProjectionDefinition<T>)doc;
    }
    
}