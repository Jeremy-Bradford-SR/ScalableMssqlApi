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

        [HttpPost("query")]
        public async Task<IActionResult> Query([FromBody] string sql)
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
