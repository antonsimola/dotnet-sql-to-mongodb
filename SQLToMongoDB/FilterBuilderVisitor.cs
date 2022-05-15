using System.Globalization;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class FilterBuilderVisitor<T> : TSqlConcreteFragmentVisitor
{
    private object currentRightHandExpression;
    private string currentFieldExpression;
    private Stack<IList<FilterDefinition<T>>> currentExpressions = new Stack<IList<FilterDefinition<T>>>();
    private FilterDefinition<T> filter = FilterDefinition<T>.Empty;
    private FilterDefinitionBuilder<T> filterBuilder = Builders<T>.Filter;


    public new FilterDefinition<T> Visit(QuerySpecification spec)
    {
        if (spec.WhereClause is null) return null;

        if (currentExpressions.Count == 0)
        {
            // there is no and or or, nothing has started a scope
            currentExpressions.Push(new List<FilterDefinition<T>>());
        }
        
        spec.WhereClause.AcceptChildren(this);

        // if there is current Expressions left, it means there was no AND or OR

        if (currentExpressions.Count > 1)
        {
            throw new Exception("shouldn't happen that stack is larger than 1 at the top level");
        }

        if (currentExpressions.Count == 1)
        {
            filter = filter & filterBuilder.And(currentExpressions.Pop());
        }

        return filter;
    }

    public override void Visit(StringLiteral node)
    {
        currentRightHandExpression = node.Value;
        base.Visit(node);
    }

    public override void Visit(NullLiteral node)
    {
        currentRightHandExpression = null;
        base.Visit(node);
    }
    


    public override void Visit(IntegerLiteral node)
    {
        currentRightHandExpression = int.Parse(node.Value);
        base.Visit(node);
    }

    public override void Visit(RealLiteral node)
    {
        currentRightHandExpression = double.Parse(node.Value, CultureInfo.InvariantCulture);
        base.Visit(node);
    }

    public override void Visit(NumericLiteral node)
    {
        currentRightHandExpression = double.Parse(node.Value, CultureInfo.InvariantCulture);
        base.Visit(node);
    }


    public override void Visit(ColumnReferenceExpression node)
    {
        currentFieldExpression = string.Join(".", node.MultiPartIdentifier.Identifiers.Select(s => s.Value));
        Console.WriteLine(currentFieldExpression);
        base.Visit(node);
    }

    public override void ExplicitVisit(BooleanParenthesisExpression node)
    {
        base.ExplicitVisit(node);
        //EndGroup();
    }

    public override void ExplicitVisit(BooleanBinaryExpression node)
    {
        currentExpressions.Push(new List<FilterDefinition<T>>());
        base.ExplicitVisit(node);

        var curExprs = currentExpressions.Pop();
        foreach (var expr in curExprs)
        {
            if (node.BinaryExpressionType == BooleanBinaryExpressionType.And)
                filter &= expr;
            else
                filter |= expr;
        }


        // filter = filter & (node.BinaryExpressionType == BooleanBinaryExpressionType.And
        //     ? filterBuilder.And(curExprs)
        //     : filterBuilder.Or(curExprs));
    }

    public override void ExplicitVisit(BooleanComparisonExpression node)
    {
        base.ExplicitVisit(node);
        
        
        FilterDefinition<T> def = node.ComparisonType switch
        {
            BooleanComparisonType.Equals => filterBuilder.Eq(currentFieldExpression,
                currentRightHandExpression),
            BooleanComparisonType.GreaterThan => filterBuilder.Gt(currentFieldExpression,
                currentRightHandExpression),
            BooleanComparisonType.LessThan => filterBuilder.Lt(currentFieldExpression,
                currentRightHandExpression),
            BooleanComparisonType.GreaterThanOrEqualTo => filterBuilder.Gte(currentFieldExpression,
                currentRightHandExpression),
            BooleanComparisonType.LessThanOrEqualTo => filterBuilder.Lte(currentFieldExpression,
                currentRightHandExpression),
            BooleanComparisonType.NotEqualToBrackets or BooleanComparisonType.NotEqualToExclamation =>
                filterBuilder.Ne(currentFieldExpression, currentRightHandExpression),
            BooleanComparisonType.NotLessThan => filterBuilder.Not(
                filterBuilder.Lt(currentFieldExpression, currentRightHandExpression)),
            BooleanComparisonType.NotGreaterThan => filterBuilder.Not(
                filterBuilder.Gt(currentFieldExpression, currentRightHandExpression)),
            BooleanComparisonType.LeftOuterJoin => throw new ArgumentOutOfRangeException(node.ComparisonType + ""),
            BooleanComparisonType.RightOuterJoin => throw new ArgumentOutOfRangeException(node.ComparisonType + ""),
            _ => throw new ArgumentOutOfRangeException(node.ComparisonType + "")
        };
        currentExpressions.Peek().Add(def);
    }

    public override void ExplicitVisit(BooleanIsNullExpression node)
    {
        base.ExplicitVisit(node);
        if (node.IsNot)
        {
            currentExpressions.Peek().Add(filterBuilder.Not(filterBuilder.Eq(currentFieldExpression, (object)null)));
        }
        else
        {
            currentExpressions.Peek().Add(filterBuilder.Eq(currentFieldExpression, (object)null));
        }
    }
}