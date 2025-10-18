# API Documentation

This document describes the available HTTP endpoints, request/response examples, and behavior specific to this sample API.

Base URL: http://localhost:8083/

## GET /api/ping

Health check endpoint. Returns a simple string body `pong`.

Response:

Status: 200 OK

Body: text/plain

```text
pong
```

## GET /api/data/tables

Returns the list of base table names in the configured database (from INFORMATION_SCHEMA.TABLES).

Response:

Status: 200 OK

Body (application/json) - array of table name strings.

Example:

```json
["Customers","Orders","Products"]
```

## GET /api/data/schema?table={tableName}

Query string: `table` (required) - the table name to inspect. The controller issues `SELECT TOP 0 * FROM [table]` to obtain schema information.

Response: 200 OK

Body: JSON array of column metadata objects with the following fields:

- ColumnName
- DataType (SQL Server data type name as returned by GetSchema)
- AllowDBNull
- IsIdentity
- IsKey
- Skippable (boolean) - whether the API will skip this column type from ad-hoc query results (e.g. geometry, varbinary, image)

Example:

```json
[
  {
    "ColumnName": "Id",
    "DataType": "int",
    "AllowDBNull": false,
    "IsIdentity": true,
    "IsKey": true,
    "Skippable": false
  },
  {
    "ColumnName": "Shape",
    "DataType": "geometry",
    "AllowDBNull": true,
    "IsIdentity": false,
    "IsKey": false,
    "Skippable": true
  }
]
```

## POST /api/data/query

Executes arbitrary SQL provided in the request body as a raw JSON string (not an object). Example request body: `"SELECT TOP 10 * FROM dbo.MyTable"`.

Request:

- Content-Type: application/json
- Body: a JSON string containing the SQL to execute

Response: 200 OK

Body: JSON object with keys:

- `sql`: the SQL string that was executed
- `skippedColumns`: object mapping column name -> data type that were skipped due to unsupported types
- `data`: array of row objects (column name -> value). Unsupported types will either be skipped (not shown) or marked with a placeholder string.

Example response:

```json
{
  "sql": "SELECT TOP 2 Id, Name, Shape FROM dbo.MyTable",
  "skippedColumns": { "Shape": "geometry" },
  "data": [
    { "Id": 1, "Name": "Alice" },
    { "Id": 2, "Name": "Bob" }
  ]
}
```

Security note: This endpoint executes raw SQL and is intended for internal tooling or admin use only. Do not expose it without authentication and authorization. Consider restricting the allowed SQL commands or using parameterized stored procedures for production scenarios.
