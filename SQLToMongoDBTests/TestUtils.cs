

using Newtonsoft.Json;
using NUnit.Framework;

namespace SQLToMongoDBTests;

public static class TestUtils
{
    public static void AssertJsonEqual(object expect, object actual)
    {
        var expectJson = JsonConvert.SerializeObject(expect);
        var actualJson = JsonConvert.SerializeObject(actual);
        Assert.AreEqual(expectJson, actualJson);
    }
}