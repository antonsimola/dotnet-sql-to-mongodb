using System.Globalization;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson;

namespace SQLToMongoDB;

public class ExpressionVisitor<T> : TSqlConcreteFragmentVisitor
{
    private Stack<object?> SubExpressions = new();

    private string? AsFieldName { get; set; }
    private string? FieldName { get; set; }
    private string? FunctionName { get; set; }

    public BsonDocument GetAsBsonDocument()
    {
        return (BsonDocument)GetAsObject();
    }

    public BsonValue GetAsBsonValue()
    {
        return BsonValue.Create(GetAsObject());
    }

    // if it can be simple raw value, like "hello" or 2, then shouldn't cast

    public object? GetAsObject()
    {
        if (SubExpressions.Count is > 1 or 0)
        {
            throw new Exception("Stack is not consumed, you are probably using some unsupported function");
        }

        return SubExpressions.Pop();
    }

    public string? GetColumnNewName()
    {
        return AsFieldName ?? (FieldName == "*" ? FunctionName : FieldName);
    }

    public string? GetColumnName()
    {
        return FieldName;
    }

    private string EscapeDots(string str)
    {
        return str.Replace(".", "_");
    }


    public override void ExplicitVisit(BooleanTernaryExpression node)
    {
        //base.Visit(node); //Do the visiting by ourselves

        var testVisitor = new ExpressionVisitor<T>();
        var leftVisitor = new ExpressionVisitor<T>();
        var rightVisitor = new ExpressionVisitor<T>();
        node.FirstExpression.Accept(testVisitor); //accept children??
        node.SecondExpression.Accept(leftVisitor); //accept children??
        node.ThirdExpression.Accept(rightVisitor); //accept children??

        var leftExpr = leftVisitor.GetAsObject();
        var rightExpr = rightVisitor.GetAsObject();
        var testExpr = testVisitor.GetAsObject();

        var inner = new BsonArray()
        {
            new BsonDocument("$gte", new BsonArray(new[] { testExpr, leftExpr })),
            new BsonDocument("$lte", new BsonArray(new[] { testExpr, rightExpr }))
        };

        var subExpr = new BsonDocument("$and", inner);
        if (node.TernaryExpressionType == BooleanTernaryExpressionType.Between)
        {
            SubExpressions.Push(subExpr);
        }
        else if (node.TernaryExpressionType == BooleanTernaryExpressionType.NotBetween)
        {
            SubExpressions.Push(new BsonDocument("$not", subExpr));
        }
        else
        {
            throw new NotImplementedException(node.TernaryExpressionType.ToString());
        }
    }

    public override void Visit(ColumnReferenceExpression node)
    {
        if (node.ColumnType == ColumnType.Wildcard)
        {
            FieldName = "*";
            SubExpressions.Push($"$$ROOT");
        }
        else
        {
            var path = string.Join(".", node.MultiPartIdentifier.Identifiers.Select(s => s.Value));
            SubExpressions.Push($"${path}");
            FieldName = path;
        }

        base.Visit(node);
    }

    public override void Visit(IdentifierOrValueExpression e)
    {
        AsFieldName = e?.Value;
        base.Visit(e);
    }

    public override void Visit(StringLiteral node)
    {
        SubExpressions.Push(node.Value);
        base.Visit(node);
    }

    public override void Visit(NullLiteral node)
    {
        SubExpressions.Push(null);
        base.Visit(node);
    }


    public override void Visit(IntegerLiteral node)
    {
        SubExpressions.Push(int.Parse(node.Value));
        base.Visit(node);
    }

    public override void Visit(RealLiteral node)
    {
        SubExpressions.Push(double.Parse(node.Value, CultureInfo.InvariantCulture));
        base.Visit(node);
    }

    public override void Visit(NumericLiteral node)
    {
        SubExpressions.Push(double.Parse(node.Value, CultureInfo.InvariantCulture));
        base.Visit(node);
    }

    public override void ExplicitVisit(BooleanBinaryExpression node)
    {
        base.ExplicitVisit(node);

        var left = SubExpressions.Pop();
        var right = SubExpressions.Pop();
        SubExpressions.Push(new BsonDocument(
            node.BinaryExpressionType == BooleanBinaryExpressionType.And ? "$and" : "$or",
            new BsonArray(new[] { left, right })));
    }

    public override void ExplicitVisit(BooleanComparisonExpression node)
    {
        //base.ExplicitVisit(node);
        var leftVisitor = new ExpressionVisitor<T>();
        var rightVisitor = new ExpressionVisitor<T>();
        node.FirstExpression.Accept(leftVisitor);
        node.SecondExpression.Accept(rightVisitor);

        var left = leftVisitor.GetAsObject();
        var right = rightVisitor.GetAsObject();

        BsonDocument expr = node.ComparisonType switch
        {
            BooleanComparisonType.Equals => new BsonDocument("$eq", new BsonArray(new[] { left, right })),
            BooleanComparisonType.GreaterThan => new BsonDocument("$gt", new BsonArray(new[] { left, right })),
            BooleanComparisonType.LessThan => new BsonDocument("$lt", new BsonArray(new[] { left, right })),
            BooleanComparisonType.GreaterThanOrEqualTo =>
                new BsonDocument("$gte", new BsonArray(new[] { left, right })),
            BooleanComparisonType.LessThanOrEqualTo => new BsonDocument("$lte", new BsonArray(new[] { left, right })),
            BooleanComparisonType.NotEqualToBrackets or BooleanComparisonType.NotEqualToExclamation =>
                new BsonDocument("$ne", new BsonArray(new[] { left, right })),
            BooleanComparisonType.NotLessThan => new BsonDocument("not",
                new BsonDocument("$lt", new BsonArray(new[] { left, right }))),
            BooleanComparisonType.NotGreaterThan => new BsonDocument("not",
                new BsonDocument("gt", new BsonArray(new[] { left, right }))),
            BooleanComparisonType.LeftOuterJoin => throw new NotImplementedException(node.ComparisonType + ""),
            BooleanComparisonType.RightOuterJoin => throw new NotImplementedException(node.ComparisonType + ""),
            _ => throw new ArgumentOutOfRangeException(node.ComparisonType + "")
        };
        SubExpressions.Push(expr);
    }

    public override void ExplicitVisit(BooleanIsNullExpression node)
    {
        base.ExplicitVisit(node);
        var expr = SubExpressions.Pop();

        if (node.IsNot)
        {
            SubExpressions.Push(new BsonDocument("$not",
                new BsonDocument("$eq", new BsonArray(new[] { expr, null }))));
        }
        else
        {
            SubExpressions.Push(
                new BsonDocument("$eq", new BsonArray(new[] { expr, null })));
        }
    }

    public override void ExplicitVisit(FunctionCall e)
    {
        var funcLower = e.FunctionName.Value.ToLowerInvariant();

        FunctionName = e.FunctionName.Value;
        
        
        base.ExplicitVisit(e); // this will visit the parameters, they are pushed to stack

        var parameters = new List<object>();

        for (int i = 0; i < e.Parameters.Count; i++)
        {
            parameters.Insert(0, SubExpressions.Pop());
        }

        
        
        SubExpressions.Push(SupportedFunctions.Functions[funcLower](funcLower, parameters.ToArray()));
    }

    public override void ExplicitVisit(BinaryExpression node)
    {
        var type = node.BinaryExpressionType;
        base.Visit(node);

        var leftVisitor = new ExpressionVisitor<T>();
        var rightVisitor = new ExpressionVisitor<T>();
        node.FirstExpression.Accept(leftVisitor);
        node.SecondExpression.Accept(rightVisitor);
        var left = leftVisitor.GetAsObject();
        var right = rightVisitor.GetAsObject();

        var oper = type switch
        {
            BinaryExpressionType.Add => "$add",
            BinaryExpressionType.Subtract => "$subtract",
            BinaryExpressionType.Multiply => "$multiply",
            BinaryExpressionType.Divide => "$divide",
            BinaryExpressionType.Modulo => "$mod",
            BinaryExpressionType.BitwiseAnd => throw new ArgumentOutOfRangeException(type.ToString()),
            BinaryExpressionType.BitwiseOr => throw new ArgumentOutOfRangeException(type.ToString()),
            BinaryExpressionType.BitwiseXor => throw new ArgumentOutOfRangeException(type.ToString()),
            _ => throw new ArgumentOutOfRangeException(type.ToString())
        };
        SubExpressions.Push(new BsonDocument(oper, new BsonArray(new[] { left, right })));
    }
}