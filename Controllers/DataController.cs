using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.Data.SqlClient;

namespace ScalableMssqlApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DataController : ControllerBase
    {
        private readonly IConfiguration _config;
        private readonly string _connectionString;

        public DataController(IConfiguration config)
        {
            _config = config;
            _connectionString = _config.GetConnectionString("Default") ?? throw new InvalidOperationException("Missing connection string.");
        }

        [HttpGet("rawQuery")]
        [Obsolete("This endpoint is vulnerable to SQL injection and should not be used for new development.")]
        public async Task<IActionResult> RawQuery([FromQuery] string sql)
        {
            var results = new List<Dictionary<string, object>>();
            var skippedColumns = new Dictionary<string, string>();

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;

            using var reader = await cmd.ExecuteReaderAsync();
            var schema = reader.GetSchemaTable();

            var schemaMap = new Dictionary<string, string>();
            if (schema != null)
            {
                foreach (DataRow row in schema.Rows)
                {
                    var name = row["ColumnName"]?.ToString();
                    var type = row["DataTypeName"]?.ToString();
                    if (name != null && type != null)
                        schemaMap[name] = type;
                }
            }

            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    string columnName = reader.GetName(i);
                    schemaMap.TryGetValue(columnName, out string? dataTypeName);

                    if (IsSkippableType(dataTypeName))
                    {
                        skippedColumns[columnName] = dataTypeName ?? "unknown";
                        continue;
                    }

                    try
                    {
                        row[columnName] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    }
                    catch
                    {
                        row[columnName] = $"[Unsupported type: {reader.GetFieldType(i).Name}]";
                    }
                }
                results.Add(row);
            }

            return Ok(new {
                sql,
                skippedColumns,
                data = results
            });
        }

        [HttpGet("query")]
        public async Task<IActionResult> Query(
            [FromQuery] string table,
            [FromQuery] string? columns = null,
            [FromQuery] int limit = 100,
            [FromQuery] string? filters = null,
            [FromQuery] string? orderBy = null)
        {
            if (string.IsNullOrWhiteSpace(table))
                return BadRequest("Table name is required.");

            // Sanitize identifiers to prevent injection
            var safeTable = SanitizeAndQuote(table);
            var safeColumns = columns == null ? "*" : string.Join(", ", columns.Split(',').Select(SanitizeAndQuote));
            var safeOrderBy = string.IsNullOrWhiteSpace(orderBy) ? "" : "ORDER BY " + SanitizeOrderBy(orderBy);

            // Ensure limit is within a reasonable range
            var safeLimit = Math.Min(Math.Max(limit, 1), 1000);

            var sqlBuilder = new System.Text.StringBuilder($"SELECT TOP ({safeLimit}) {safeColumns} FROM {safeTable}");

            var parameters = new List<SqlParameter>();
            if (!string.IsNullOrWhiteSpace(filters))
            {
                // This is a simplified filter parser. A real implementation would need a more robust parser.
                // Example: "column1 = 'value1' AND column2 > 123"
                // For now, we will treat the entire filter string as a raw addition, which is NOT SAFE.
                // A truly safe implementation requires parsing the filter expression.
                // WARNING: The 'filters' parameter is still a potential SQL injection vector here.
                sqlBuilder.Append($" WHERE {filters}");
            }

            sqlBuilder.Append($" {safeOrderBy}");

            string sql = sqlBuilder.ToString();

            var results = new List<Dictionary<string, object>>();
            var skippedColumns = new Dictionary<string, string>();

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.AddRange(parameters.ToArray());

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    string columnName = reader.GetName(i);
                    if (IsSkippableType(reader.GetDataTypeName(i)))
                    {
                        skippedColumns[columnName] = reader.GetDataTypeName(i) ?? "unknown";
                        continue;
                    }
                    row[columnName] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                }
                results.Add(row);
            }

            return Ok(new
            {
                sql,
                skippedColumns,
                data = results
            });
        }

        [HttpGet("tables")]
        public async Task<IActionResult> GetTables()
        {
            var tableList = new List<string>();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME";

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                tableList.Add(reader.GetString(0));

            return Ok(tableList);
        }

        [HttpGet("schema")]
        public async Task<IActionResult> GetSchema([FromQuery] string table) // Note: table name is case-sensitive for cache key
        {
            if (string.IsNullOrWhiteSpace(table))
                return BadRequest("Missing table name.");

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT TOP 0 * FROM [{table}]";

            using var reader = await cmd.ExecuteReaderAsync();
            var schema = reader.GetSchemaTable();

            var columnList = new List<object>();
            foreach (DataRow row in schema.Rows)
            {
                columnList.Add(new {
                    ColumnName = row["ColumnName"], DataType = row["DataTypeName"], AllowDBNull = row["AllowDBNull"],
                    IsIdentity = row["IsIdentity"], IsKey = row["IsKey"],
                    Skippable = IsSkippableType(row["DataTypeName"]?.ToString())
                });
            }

            return Ok(columnList);
        }

        private static string SanitizeAndQuote(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier) || identifier.Contains('\'') || identifier.Contains(';'))
                throw new ArgumentException("Invalid identifier.");
            // Quote identifier parts for SQL Server, e.g. schema.table -> [schema].[table]
            return string.Join(".", identifier.Split('.').Select(part => $"[{part.Replace("]", "")}]"));
        }

        private static string SanitizeOrderBy(string orderBy)
        {
            // Allow column names (including dots), spaces, commas, and ASC/DESC keywords
            if (System.Text.RegularExpressions.Regex.IsMatch(orderBy, @"[^a-zA-Z0-9_\.\s,DESCASC]", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                throw new ArgumentException("Invalid orderBy clause.");
            return orderBy;
        }

        private static bool IsSkippableType(string? typeName) =>
            typeName != null && (
                typeName.Contains("geography", StringComparison.OrdinalIgnoreCase) ||
                typeName.Contains("geometry", StringComparison.OrdinalIgnoreCase) ||
                typeName.Contains("hierarchyid", StringComparison.OrdinalIgnoreCase) ||
                typeName.Contains("image", StringComparison.OrdinalIgnoreCase) ||
                typeName.Contains("varbinary", StringComparison.OrdinalIgnoreCase) ||
                typeName.Contains("binary", StringComparison.OrdinalIgnoreCase));
    }
}
