using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class GroupByBuilderVisitor<T> : TSqlConcreteFragmentVisitor
{
    private string currentField;
    private string currentAsField;
    private BsonDocument currentFunc;


    public new ProjectionDefinition<T> Visit(QuerySpecification spec)
    {
        if (spec.GroupByClause == null) return null;

        var fields = new List<BsonElement>();

        var i = 0;
        foreach (var grouping in spec.GroupByClause.GroupingSpecifications)
        {
            grouping.AcceptChildren(this);

            fields.Add(new BsonElement(EscapeDots(currentAsField ?? currentField), GetFieldExpression(currentField)));
            currentField = null;
            currentAsField = null;
        }
        
        var accumulators = new List<BsonElement>();
        
        foreach (var select in spec.SelectElements)
        {
            select.AcceptChildren(this);

            accumulators.Add(new BsonElement(EscapeDots(currentAsField ?? currentField), currentFunc));
            currentField = null;
            currentAsField = null;
        }
        BsonDocument doc = new BsonDocument()
        {
            {"_id", new BsonDocument(fields)}
        };

        foreach (var accumulator in accumulators)
        {
            doc.Add(accumulator);
        }
        
        


        return (ProjectionDefinition<T>) doc;
    }

    public string GetFieldExpression(string field)
    {
        return $"${field}";
    }


    public string EscapeDots(string str)
    {
        return str.Replace(".", "_");
    }
    
    public override void ExplicitVisit(FunctionCall e)
    {
        var func = e.FunctionName.Value.ToLowerInvariant() switch
        {
            "sum" => "$sum",
            "avg" => "$avg",
            "min" => "$min",
            "max" => "$max",
            _ => throw new ArgumentOutOfRangeException(e.FunctionName.Value)
        };
        base.ExplicitVisit(e);
    currentFunc = new BsonDocument(func, GetFieldExpression(currentField)); 
        
    }

    public override void Visit(IdentifierOrValueExpression e)
    {
        Console.WriteLine(e);

        currentAsField = e?.Value;
        base.Visit(e);
    }

    public override void Visit(ColumnReferenceExpression e)
    {
        currentField = string.Join(".", e.MultiPartIdentifier.Identifiers.Select(s => s.Value));
        base.Visit(e);
    }
}