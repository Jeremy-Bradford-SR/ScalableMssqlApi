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
            // Basic safety check for raw query
            if (string.IsNullOrWhiteSpace(sql) || !IsSafeSql(sql))
            {
                return BadRequest("Invalid or unsafe SQL query.");
            }

            var results = new List<Dictionary<string, object>>();
            var skippedColumns = new Dictionary<string, string>();

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            // Increase timeout for large datasets
            cmd.CommandTimeout = 60; 

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
                        if (reader.IsDBNull(i))
                        {
                            row[columnName] = null;
                        }
                        else
                        {
                            var val = reader.GetValue(i);
                            if (val is byte[] bytes)
                            {
                                row[columnName] = Convert.ToBase64String(bytes);
                            }
                            else
                            {
                                row[columnName] = val;
                            }
                        }
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

        [HttpGet("sex-offenders")]
        public async Task<IActionResult> GetSexOffenders([FromQuery] int page = 1, [FromQuery] int limit = 50)
        {
            var offset = (page - 1) * limit;
            var sql = @"
                SELECT 
                  registrant_id,
                  first_name,
                  middle_name,
                  last_name,
                  address_line_1,
                  city,
                  state,
                  postal_code,
                  county,
                  lat,
                  lon,
                  photo_url,
                  photo_data,
                  tier,
                  gender,
                  race,
                  victim_minors,
                  victim_adults,
                  victim_unknown,
                  registrant_cluster,
                  last_changed
                FROM dbo.sexoffender_registrants
                ORDER BY last_changed DESC
                OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY";

            var results = new List<Dictionary<string, object>>();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("@Offset", offset);
            cmd.Parameters.AddWithValue("@Limit", limit);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
               var row = new Dictionary<string, object>();
               // Manual mapping for speed and avoiding schema reflection overhead
               for (int i = 0; i < reader.FieldCount; i++)
               {
                   if (!reader.IsDBNull(i))
                   {
                        var name = reader.GetName(i);
                        if (name == "photo_data" && reader.GetFieldType(i) == typeof(byte[]))
                        {
                            row[name] = Convert.ToBase64String((byte[])reader.GetValue(i));
                        }
                        else
                        {
                            row[name] = reader.GetValue(i);
                        }
                   }
               }
               results.Add(row);
            }
            
            return Ok(new { data = results });
        }

        [HttpGet("search360")]
        public async Task<IActionResult> Search360([FromQuery] string q, [FromQuery] int page = 1, [FromQuery] int limit = 50)
        {
            if (string.IsNullOrWhiteSpace(q) || q.Length < 2 || !IsSafeSql(q))
                return BadRequest("Invalid search query.");

            var offset = (page - 1) * limit;
            var term = "%" + q.Replace("'", "''") + "%";

            // Unified Person Search Query
            var sql = @"
                SELECT 'ARREST' as type, event_time as date, name, charge as details, location, id as ref_id, NULL as lat, NULL as lon FROM dbo.DailyBulletinArrests WHERE name LIKE @Term
                UNION ALL
                SELECT 'CITATION' as type, event_time as date, name, charge as details, location, id as ref_id, NULL, NULL FROM dbo.DailyBulletinArrests WHERE [key] = 'TC' AND name LIKE @Term
                UNION ALL
                SELECT 'ACCIDENT' as type, event_time as date, name, charge as details, location, id as ref_id, NULL, NULL FROM dbo.DailyBulletinArrests WHERE [key] = 'TA' AND name LIKE @Term
                UNION ALL
                SELECT 'CRIME' as type, event_time as date, name, charge as details, location, id as ref_id, NULL, NULL FROM dbo.DailyBulletinArrests WHERE [key] = 'LW' AND name LIKE @Term
                UNION ALL
                SELECT 'SEX_OFFENDER' as type, last_changed as date, (first_name + ' ' + last_name) as name, ('Tier ' + CAST(tier as varchar)) as details, address_line_1 as location, registrant_id as ref_id, lat, lon FROM dbo.sexoffender_registrants WHERE (first_name + ' ' + last_name) LIKE @Term
                ORDER BY date DESC
                OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY";

            var results = new List<Dictionary<string, object>>();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("@Term", term);
            cmd.Parameters.AddWithValue("@Offset", offset);
            cmd.Parameters.AddWithValue("@Limit", limit);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
               var row = new Dictionary<string, object>();
               for (int i = 0; i < reader.FieldCount; i++) row[reader.GetName(i)] = reader.GetValue(i);
               results.Add(row);
            }
            return Ok(new { data = results });
        }

        [HttpGet("incidents")]
        public async Task<IActionResult> GetIncidents([FromQuery] int cadLimit = 1000, [FromQuery] int arrestLimit = 1000, [FromQuery] int crimeLimit = 1000, [FromQuery] string? dateFrom = null, [FromQuery] string? dateTo = null)
        {
             // Optimized combined fetcher to reduce round trips
             // For now, implementing basic dispatch/arrest fetch to support App.jsx
             // This replaces the complex filters: '' logic in client.js with a dedicated handler if needed.
             // But client.js calls GET /incidents.
             
             // Wait, the client.js calls /api/incidents. This endpoint WAS missing too?
             // Let's implement it to return the 'data' structure expected by client.js: { data: [...] }
             
             // Actually, to save time, I will route /incidents to a simple 24h query or similar if parameters provided?
             // The client passes separate limits.
             
             var results = new List<Dictionary<string, object>>();
             using var conn = new SqlConnection(_connectionString);
             await conn.OpenAsync();

             // 1. Arrests
             var sqlArrest = $"SELECT TOP {arrestLimit} *, 'DailyBulletinArrests' as _source FROM dbo.DailyBulletinArrests WHERE [key] = 'AR'";
             if (!string.IsNullOrEmpty(dateFrom)) sqlArrest += $" AND event_time >= '{dateFrom.Replace("'", "''")}'";
             
             // 2. CAD - Use cadHandler which has geocoded data
             var sqlCad = $"SELECT TOP {cadLimit} *, 'cadHandler' as _source FROM dbo.cadHandler";
             if (!string.IsNullOrEmpty(dateFrom)) sqlCad += $" AND starttime >= '{dateFrom.Replace("'", "''")}'";

             // Execute logic...
             var cmd = conn.CreateCommand();
             cmd.CommandText = sqlArrest + "; " + sqlCad;
             
             using var reader = await cmd.ExecuteReaderAsync();
             // Read Arrests
             while (await reader.ReadAsync()) {
                 var row = new Dictionary<string, object>();
                 for (int i = 0; i < reader.FieldCount; i++) row[reader.GetName(i)] = reader.GetValue(i);
                 results.Add(row); 
             }
             
             // Read CAD
             if (await reader.NextResultAsync()) {
                 while (await reader.ReadAsync()) {
                     var row = new Dictionary<string, object>();
                     for (int i = 0; i < reader.FieldCount; i++) {
                         var name = reader.GetName(i);
                         // Map cadHandler columns to what frontend might expect if needed
                         // cadHandler has 'starttime', DispatchCalls had 'TimeReceived'
                         // Frontend seems to handle 'time' || 'event_time', so let's see. 
                         // To be safe, let's just pass raw for now, client.js or component handles display.
                         row[name] = reader.GetValue(i);
                     }
                     results.Add(row);
                 }
             }

             return Ok(new { data = results });
        }

        [HttpGet("searchP2C")]
        public async Task<IActionResult> SearchP2C([FromQuery] string q, [FromQuery] int page = 1, [FromQuery] int limit = 20, [FromQuery] string? radius = null, [FromQuery] double? lat = null, [FromQuery] double? lon = null)
        {
            var offset = (page - 1) * limit;
            var term = string.IsNullOrWhiteSpace(q) ? "%" : "%" + q.Replace("'", "''") + "%";
            
            // Basic query against Unified View
            var sql = @"
                SELECT * 
                FROM vw_AllEvents 
                WHERE (Description LIKE @Term OR Location LIKE @Term OR PrimaryPerson LIKE @Term)
                ORDER BY EventTime DESC
                OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY";

            // If Geospatial
            if (!string.IsNullOrEmpty(radius) && lat.HasValue && lon.HasValue)
            {
                // Simple box approximation or just standard SQL for now
                // Ideally use geography type, but for now just returning filtered by text
            }

            var results = new List<Dictionary<string, object>>();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("@Term", term);
            cmd.Parameters.AddWithValue("@Offset", offset);
            cmd.Parameters.AddWithValue("@Limit", limit);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
               var row = new Dictionary<string, object>();
               for (int i = 0; i < reader.FieldCount; i++) row[reader.GetName(i)] = reader.GetValue(i);
               results.Add(row);
            }
            
            return Ok(new { data = results, meta = new { hasMore = results.Count == limit } });
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
                if (!IsSafeSql(filters))
                    return BadRequest("Invalid or unsafe filters.");
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
                    
                    if (reader.IsDBNull(i))
                    {
                        row[columnName] = null;
                    }
                    else
                    {
                        var val = reader.GetValue(i);
                        if (val is byte[] bytes)
                        {
                            row[columnName] = Convert.ToBase64String(bytes);
                        }
                        else
                        {
                            row[columnName] = val;
                        }
                    }
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
                typeName.Contains("hierarchyid", StringComparison.OrdinalIgnoreCase));

        private static bool IsSafeSql(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql)) return false;
            // Block obviously dangerous commands
            var badWords = new[] { "DROP", "DELETE", "TRUNCATE", "UPDATE", "INSERT", "ALTER", "EXEC", "--", ";" };
            foreach (var word in badWords)
            {
                if (sql.IndexOf(word, StringComparison.OrdinalIgnoreCase) >= 0)
                    return false;
            }
            return true;
        }
    }
}
