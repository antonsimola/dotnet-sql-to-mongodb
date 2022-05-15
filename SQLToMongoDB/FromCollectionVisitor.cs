using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SQLToMongoDB;

public class FromCollectionVisitor: TSqlConcreteFragmentVisitor
{

    public string CollectionName { get; set; }
    
    public override void Visit(NamedTableReference node)
    {
        CollectionName = node.SchemaObject.BaseIdentifier.Value;
        //base.Visit(node);
    }

    public new string Visit(QuerySpecification spec)
    {
        spec.FromClause.Accept(this);
        return CollectionName;
    }
}