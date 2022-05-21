using System.Globalization;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

namespace SQLToMongoDB;

public class FilterBuilderVisitor<T> : TSqlConcreteFragmentVisitor
{


    public new FilterDefinition<T> Visit(QuerySpecification spec)
    {
        if (spec.WhereClause is null) return null;
        var expressionVisitor =  new ExpressionVisitor<T>();
        spec.WhereClause.AcceptChildren(expressionVisitor);
        return  new BsonDocument("$expr", expressionVisitor.GetAsBsonDocument());
    }
}