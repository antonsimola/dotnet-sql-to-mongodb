using DnsClient.Internal;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SQLToMongoDB;


public class JoinDefinition
{
    public string LocalTable { get; set; }
    public string FromTable { get; set; }
    public string LocalField { get; set; }
    public string ForeignField { get; set; }
    public string AsField { get; set; }
}

public class JoinVisitor<T>: TSqlConcreteFragmentVisitor
{

    private string localTable;
    private string fromTable;
    private string localField;
    private string foreignField;
    private string asField;


    private bool local;
    public new JoinDefinition Visit(QuerySpecification spec)
    {
        spec.FromClause.AcceptChildren(this);

        if (localTable == null || fromTable == null)
        {
            return null;
        }  
        
        return new JoinDefinition()
        {
            AsField = asField,
            ForeignField = foreignField,
            FromTable = fromTable,
            LocalField = localField,
            LocalTable = localTable
        };
        
        
    }


    public override void Visit(NamedTableReference tableRef)
    {
        if (local)
        {
            localTable =  string.Join(".", tableRef.SchemaObject.Identifiers.Select(i => i.Value));    
        }
        else
        {
            fromTable =  string.Join(".", tableRef.SchemaObject.Identifiers.Select(i => i.Value));
            asField = tableRef.Alias?.Value ?? fromTable;
        }
    }

    public override void Visit(ColumnReferenceExpression e)
    {

        if (e.MultiPartIdentifier.Identifiers.Select(i => i.Value).Contains(localTable))
        {
            localField = e.MultiPartIdentifier.Identifiers.LastOrDefault()?.Value;            
        }
        else
        {
            foreignField = e.MultiPartIdentifier.Identifiers.LastOrDefault()?.Value;
        }




    }
    
    

    public override void ExplicitVisit(QualifiedJoin join)
    {
        join.SecondTableReference.Accept(this);
        local = true;
        join.FirstTableReference.Accept(this);
        
        
        join.SearchCondition.Accept(this);

    }
}