using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class ProjectionBuilderVisitor<T> : TSqlConcreteFragmentVisitor
{
    private BsonElement currentProjection;
    public bool AllFields { get; set; }

    private ProjectionDefinition<T> def;

    private string? currentField;
    private string? currentAsField;
    public bool ExcludeIdByDefault { get; set; } = true;

    public new ProjectionDefinition<T>? Visit(QuerySpecification spec)
    {
        var fields = new List<BsonElement>();

        foreach (var select in spec.SelectElements)
        {
            if (select is SelectStarExpression)
            {
                return null;
            }
            
            
            select.AcceptChildren(this);

            fields.Add(new BsonElement(currentAsField ?? currentField, GetFieldExpression(currentField)));
            currentField = null;
            currentAsField = null;
        }

        if (ExcludeIdByDefault)
        {
            fields.Add(new BsonElement("_id", 0));
        }

        return Builders<T>.Projection.Combine(new BsonDocument(fields));
    }

    public string GetFieldExpression(string field)
    {
        return $"${field}";
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