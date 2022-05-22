using MongoDB.Bson;

namespace SQLToMongoDB;

public static class SupportedFunctions
{
    
    private static Func<string, object[], object> NoParameters = (string func, object[] parameters) =>
        new BsonDocument($"${GetProperCasing(func)}", new BsonDocument());
    private static Func<string, object[], object> SingleParameter = (string func, object[] parameters) =>
        new BsonDocument($"${GetProperCasing(func)}", BsonValue.Create(parameters[0]));

    private static Func<string, object[], object> MultiParameters = (string func, object[] parameters) =>
        new BsonDocument($"${GetProperCasing(func)}", new BsonArray(parameters));

    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToParts/
    public static readonly IDictionary<string, Func<string, object[], object>> Functions =
        new Dictionary<string, Func<string, object[], object>>(
            StringComparer.OrdinalIgnoreCase
        )
        {
            { "abs", SingleParameter },
            { "acos", SingleParameter },
            { "acosh", SingleParameter },
            { "add", MultiParameters },
            { "addToSet", SingleParameter },
            { "allElementsTrue", MultiParameters },

            //{ "and",MultiParameters }, its a keyword
            { "anyElementsTrue", MultiParameters },
            { "arrayElemAt", MultiParameters },
            { "arrayToObject", MultiParameters },
            { "asin", SingleParameter },
            { "asinh", SingleParameter },
            { "atan", SingleParameter },
            { "atan2", SingleParameter },
            { "atanh", SingleParameter },

            { "binarySize", SingleParameter },
            { "bsonSize", SingleParameter },
            { "ceil", SingleParameter },
            { "comp", MultiParameters },
            { "concat", MultiParameters },
            { "concatArrays", MultiParameters },
            //{ "cond",MultiParameters}, TODO probably should be handled separately
            //{ "convert",MultiParameters}, TODO probably should be handled separately, casting

            { "cos", SingleParameter },
            { "cosh", SingleParameter },
            { "covariancePop", MultiParameters },
            { "covarianceSamp", MultiParameters },
            //{ "dateAdd",MultiParameters}, //TODO should have named arguments
            //TODO lot of date stuff
            { "degreesToRadians", SingleParameter },
            //{ "denseRank",SingleParameter},
            //{ "derivative",SingleParameter},
            { "exp", SingleParameter },
            //{"expMovingAvg", SingleParameter},
            //{"filter", SingleParameter},
            { "first", SingleParameter },
            { "floor", SingleParameter },
            //{"function", SingleParameter},
            //{"getField", SingleParameter},
            { "indexOfCP", MultiParameters },
            //{"integral",MultiParameters},
            { "isArray", SingleParameter },
            { "isNumber", SingleParameter },
            { "isoDayOfWeek", SingleParameter },
            { "isoWeek", SingleParameter },
            { "isoWeekYear", SingleParameter },
            { "last", SingleParameter },
            { "ln", SingleParameter },
            { "log", SingleParameter },
            { "log10", SingleParameter },
            { "pow", MultiParameters },
            { "push", SingleParameter },
            { "radiansToDegrees", SingleParameter },
            { "rand", NoParameters },
            { "range", MultiParameters },
            { "reverseArray", SingleParameter },
            { "round", MultiParameters },
            { "sampleRate",SingleParameter },
            { "setDifference",MultiParameters },
            { "setEquals",MultiParameters },
            { "setIntersection",MultiParameters },
            { "setIsSubset",MultiParameters },
            { "setUnion",MultiParameters },
            { "size",SingleParameter },
            { "sin",SingleParameter },
            { "sinh",SingleParameter },
            { "slice",MultiParameters },
            { "split",MultiParameters },
            { "sqrt",SingleParameter },
            { "stdDevPop",SingleParameter },
            { "stdDevSamp",SingleParameter },
            { "strcasecmp",SingleParameter },
            { "strLenBytes",SingleParameter },
            { "strLenCP",SingleParameter },
            { "substr",SingleParameter },
            { "substrBytes",SingleParameter },
            
            { "substrCP",SingleParameter },
            //{ "switch",SingleParameter },TODO
            { "tan",SingleParameter },
            { "tanh",SingleParameter },
            { "toBool",SingleParameter },
            { "toDate",SingleParameter },
            { "toDecimal",SingleParameter },
            { "toDouble",SingleParameter },
            { "toInt",SingleParameter },
            { "toLong",SingleParameter },
            { "toObjectId",SingleParameter },
            { "toString",SingleParameter },
            { "toLower",SingleParameter },
            { "toUpper",SingleParameter },
            { "trunc",MultiParameters },
            { "type",SingleParameter },
            { "sum", SingleParameter },
            { "avg", SingleParameter },
            { "min", SingleParameter },
            { "max", SingleParameter },
            { "year", SingleParameter },
            { "month", SingleParameter },
            { "dayOfMonth", SingleParameter },
            { "hour", SingleParameter },
            { "minute", SingleParameter },
            { "second", SingleParameter },
            { "millisecond", SingleParameter },
            { "dayOfYear", SingleParameter },
            { "dayOfWeek", SingleParameter },
            { "week", SingleParameter },
            { "count", ((s, parameters) => new BsonDocument($"$sum", 1)) },
            {
                "isodate", (s, p) => DateTime.SpecifyKind(DateTime.Parse((string)p[0]), DateTimeKind.Utc)
            },
            {
                "date", (s, p) => DateTime.SpecifyKind(DateTime.Parse((string)p[0]), DateTimeKind.Utc)
            }
        };

    private static IDictionary<string, string>? _properCasing = null;

    public static string GetProperCasing(string loweredFunc)
    {
        if (_properCasing != null)
        {
            return _properCasing[loweredFunc];
        }

        _properCasing = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var function in Functions)
        {
            _properCasing[function.Key] = function.Key;
        }

        return _properCasing[loweredFunc];
    }
}
