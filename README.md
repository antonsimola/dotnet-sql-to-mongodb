# dotnet-sql-to-mongodb

Proto for converting T-SQL SELECT queries to MongoDB aggregation pipelines.

It provides a single extension method on the IMongoDatabase interface: `db.SqlQuery<T>(sqlQuery)`

eg. 

`SELECT AVG(Age) as AvgAge from users where [Address.Street] = 'BlaaStreet' GROUP BY [Address.Street]` 

converts to 

```
[
    {
        "$match": {
            "$expr": {
                "$eq": [
                    "$Address.Street",
                    "BlaaStreet"
                ]
            }
        }
    },
    {
        "$group": {
            "_id": {
                "g0": "$Address.Street"
            },
            "AvgAge": {
                "$avg": "$Age"
            }
        }
    }
]
```

See the unit tests for more examples. Not all the unit tests pass currently, especially there are problems with group by and arrays.
