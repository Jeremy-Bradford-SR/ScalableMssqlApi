# ScalableMssqlApi

Lightweight ASP.NET Core Web API for ad-hoc SQL queries against a Microsoft SQL Server database.

This repository contains a small API that exposes a few endpoints to discover database tables, inspect table schema, and execute arbitrary SQL queries. It is container-ready (Docker) and configured to run on port 8083 by default.

## Quick facts

- Framework: .NET 8.0
- Exposes endpoints under `/api/data` and `/api/ping`
- Default port: 8083 (Kestrel and docker-compose configured)
- Connection string is read from `appsettings.json` or environment variables

 
## Files of interest

- `Program.cs` - app startup, swagger, logging, port binding
- `Controllers/DataController.cs` - main API endpoints for querying
- `Controllers/PingController.cs` - health endpoint (`/api/ping`)
- `Dockerfile`, `docker-compose.yml` - containerization

## Endpoints

The API exposes the following endpoints (see full examples in API_DOCUMENTATION.md):

- GET /api/ping -> returns `pong` (health check)
- GET /api/data/tables -> returns list of base table names
- GET /api/data/schema?table={tableName} -> returns schema details for a table
- POST /api/data/query -> executes arbitrary SQL and returns JSON results

Important: the `POST /api/data/query` endpoint will execute the SQL you send. Use responsibly and only against databases you control. No query validation or authorization is implemented in this sample.

## Configuration

The app reads the ADO.NET connection string named `Default` from the standard .NET configuration sources. By default, `appsettings.json` contains:

```json
{
  "ConnectionStrings": {
    "Default": "Server=192.168.0.43;Database=p2cdubuque;User Id=sa;Password=Thugitout09!;TrustServerCertificate=true;Max Pool Size=100;"
  }
}
```

You should override this in production using environment variables or a secrets store. For example, set the environment variable `ConnectionStrings__Default`.

## Run locally

Prerequisites: .NET 8 SDK installed, or Docker if you prefer containers.

Run with .NET SDK:

```bash
dotnet restore
dotnet run --project ScalableMssqlApi.csproj
```

This will start the API listening on `http://localhost:8083`.

Run with Docker (recommended for parity):

1. Build the image:

```bash
docker build -t scalable-mssql-api .
```

2. Run with docker-compose (maps port 8083):

```bash
docker compose up --build
```

The API will be available at `http://localhost:8083` and Swagger UI at the root (`/`).

## Example curl usage

Health check:

```bash
curl -s http://localhost:8083/api/ping
# pong
```

Get tables:

```bash
curl -s http://localhost:8083/api/data/tables | jq
```

Get schema for a table:

```bash
curl -s "http://localhost:8083/api/data/schema?table=MyTable" | jq
```

Execute an ad-hoc query (POST JSON body with SQL string):

```bash
curl -s -X POST http://localhost:8083/api/data/query \
  -H "Content-Type: application/json" \
  -d '"SELECT TOP 10 * FROM [MySchema].[MyTable]"' | jq
```

Notes about queries:

- The `query` endpoint returns a JSON object with `sql`, `skippedColumns`, and `data` keys.
- Some SQL Server types (geography, geometry, hierarchyid, image, binary/varbinary) are intentionally skipped to keep JSON simple. Skipped columns are reported in `skippedColumns`.
- NULL database values map to JSON null.

## Security and production notes

- This sample does not implement authentication or authorization. Do not expose it to untrusted networks.
- Running arbitrary SQL from HTTP requests is dangerous. If you build this for production: add authentication, input validation (or parameterized queries), and limit permissions for the SQL user.
- Consider applying connection pooling, query timeouts, and rate-limiting.

## Contributing

See `CONTRIBUTING.md` for development setup and guidelines.

## License

This project is a sample; add your preferred license file if you plan to release it publicly.
