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

## GET /api/data/incidents
Unified endpoint for fetching combined safety data (CAD, Arrests, Crimes) with time range filtering.
- **Parameters**: `cadLimit`, `arrestLimit`, `crimeLimit`, `dateFrom` (YYYY-MM-DD HH:MM:SS), `dateTo`.
- **Response**: `{ data: [ { ...rec, _source: 'cadHandler'|'DailyBulletinArrests' } ] }`

## GET /api/data/sex-offenders
Fetches the registry of Sex Offenders.
- **Optimized**: Does **not** return `photo_data` (Base64 images) to ensure fast list loading. Images must be fetched individually or via `photo_url`.
- **Parameters**: `page`, `limit`.
- **Response**: `{ data: [ { registrant_id, first_name, ... } ] }`

## GET /api/data/search360
Performs a global text search across Arrests, Citations, Accidents, Crimes, and Sex Offenders.
- **Response**: `{ data: [ { type: 'ARREST'|'CITATION'|..., name, details, ... } ] }`

---

## Architecture & Schema Implementation Notes

### `row_hash` Identity System
Historically, the database tables relied on auto-incrementing integer `id` columns or poorly scraped strings. 
This has been completely overhauled to use a deterministic MD5 hash composite key sequence.
- Primary Keys for ingested rows are now uniquely generated via `hash(raw_id + name + time + charge + location)`.
- This prevents Database duplication while simultaneously ensuring that the exact same person arrested on multiple distinct charges in the same incident isn't accidentally collapsed into a single database row.

### `tools/*` ETL Fallback Endpoints
The `ScalableMssqlApi` exposes specialized endpoints exclusively for the Python ETL post-processing orchestrators:
- `GET /api/tools/dab-time/candidates`: Used by Python to pull batches of un-parsed Daily Bulletin timestamp strings.
- `POST /api/tools/dab-time/update`: Used by Python to write a standardized `event_time` back to the row.
- `GET /api/tools/geocode/candidates`: Used to fetch batch subsets of un-geocoded addresses.
- `POST /api/tools/geocode/update`: Writes derived `latitude` and `longitude` back to the DB payload.
- `POST /api/tools/daily-bulletin/ids`: Used during Database Verification validation sweeps to ensure inserted `row_hash` IDs exist.
