using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SQLToMongoDB;

public class SkipLimitVisitor<T> : TSqlConcreteFragmentVisitor
{
    
    private int? _value;
    
    public new (int?, int?) Visit(QuerySpecification spec)
    {
        spec.TopRowFilter?.Accept(this);
        var limit = _value;
        
        _value = null;
        
     
        spec.OffsetClause?.OffsetExpression.Accept(this);
        var skip = _value;
        
        _value = null;
        
        spec.OffsetClause?.FetchExpression?.Accept(this);
        limit = _value ?? limit;
        
        return (skip, limit);
    }

    public override void Visit(NumericLiteral num)
    {
        _value = int.Parse(num.Value);
    }
    
    public override void Visit(IntegerLiteral num)
    {
        _value = int.Parse(num.Value);
    }
}