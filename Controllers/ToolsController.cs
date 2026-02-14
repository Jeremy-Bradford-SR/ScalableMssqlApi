using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Dapper;

namespace ScalableMssqlApi.Controllers
{
    [ApiController]
    [Route("api/tools")]
    public class ToolsController : ControllerBase
    {
        private readonly string _connectionString;
        private readonly ILogger<ToolsController> _logger;

        public ToolsController(IConfiguration configuration, ILogger<ToolsController> logger)
        {
            _connectionString = configuration["MssqlConnectionString"] 
                                ?? configuration["MSSQL_CONNECTION_STRING"]
                                ?? configuration.GetConnectionString("DefaultConnection")
                                ?? configuration.GetConnectionString("Default")
                                ?? Environment.GetEnvironmentVariable("MSSQL_CONNECTION_STRING");
            _logger = logger;
        }

        public class EnsureColumnsRequest {
            public string Table { get; set; }
        }

        [HttpPost("schema/ensure-geocode-columns")]
        public async Task<IActionResult> EnsureGeocodeColumns([FromBody] EnsureColumnsRequest req)
        {
            var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "cadHandler", "DailyBulletinArrests", "sexoffender_registrants" };
            if (!allowed.Contains(req.Table)) return BadRequest("Invalid table");

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            
            var sql = $@"
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{req.Table}' AND COLUMN_NAME = 'lat')
                BEGIN
                    ALTER TABLE {req.Table} ADD lat FLOAT DEFAULT 0 WITH VALUES;
                END
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{req.Table}' AND COLUMN_NAME = 'lon')
                BEGIN
                    ALTER TABLE {req.Table} ADD lon FLOAT DEFAULT 0 WITH VALUES;
                END
            ";
            await conn.ExecuteAsync(sql);
            return Ok(new { status = "Columns ensured" });
        }

        public class GeocodeCandidateDto {
            public string Id { get; set; }
            public string Address { get; set; }
        }

        [HttpGet("geocode/candidates")]
        public async Task<IActionResult> GetGeocodeCandidates([FromQuery] string table, [FromQuery] int count = 50)
        {
            if (count > 1000) count = 1000;
            using var conn = new SqlConnection(_connectionString);
            string sql = "";

            if (string.Equals(table, "cadHandler", StringComparison.OrdinalIgnoreCase)) {
                 sql = $"SELECT TOP {count} id as Id, address as Address FROM cadHandler WHERE (lat IS NULL OR (lat=0 AND lon=0)) AND address IS NOT NULL"; // Prioritize NULL, allow 0,0 retry? Maybe logic is 'lat IS NULL'. Script sets to 0 if failed. 
                 // Script logic: "lat IS NULL". If failed it sets to 0. So we only fetch NULL.
                 sql = $"SELECT TOP {count} id as Id, address as Address FROM cadHandler WHERE lat IS NULL AND address IS NOT NULL ORDER BY starttime DESC";
            } else if (string.Equals(table, "DailyBulletinArrests", StringComparison.OrdinalIgnoreCase)) {
                 sql = $"SELECT TOP {count} id as Id, location as Address FROM DailyBulletinArrests WHERE lat IS NULL AND location IS NOT NULL ORDER BY [time] DESC";
            } else if (string.Equals(table, "sexoffender_registrants", StringComparison.OrdinalIgnoreCase)) {
                 sql = $"SELECT TOP {count} registrant_id as Id, ISNULL(address_line_1,'') + ' ' + ISNULL(city,'') + ' ' + ISNULL(state,'') as Address FROM sexoffender_registrants WHERE lat IS NULL";
            } else {
                return BadRequest("Invalid table");
            }

            var res = await conn.QueryAsync<GeocodeCandidateDto>(sql);
            return Ok(res);
        }
        
        public class FetchAddressesRequest {
            public List<string> Ids { get; set; }
            public string Table { get; set; }
        }

        [HttpPost("geocode/fetch-addresses")]
        public async Task<IActionResult> FetchAddresses([FromBody] FetchAddressesRequest req) {
             if (req.Ids == null || !req.Ids.Any()) return Ok(new List<GeocodeCandidateDto>());
             
             var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "cadHandler", "DailyBulletinArrests", "sexoffender_registrants" };
             if (!allowed.Contains(req.Table)) return BadRequest("Invalid table");
             
             using var conn = new SqlConnection(_connectionString);
             string sql = "";
             if (req.Table.Equals("cadHandler", StringComparison.OrdinalIgnoreCase)) 
                 sql = $"SELECT id as Id, address as Address FROM cadHandler WHERE id IN @Ids";
             else if (req.Table.Equals("DailyBulletinArrests", StringComparison.OrdinalIgnoreCase))
                 sql = $"SELECT id as Id, location as Address FROM DailyBulletinArrests WHERE id IN @Ids";
             else 
                 sql = $"SELECT registrant_id as Id, ISNULL(address_line_1,'') + ' ' + ISNULL(city,'') + ' ' + ISNULL(state,'') as Address FROM sexoffender_registrants WHERE registrant_id IN @Ids";
                 
             var res = await conn.QueryAsync<GeocodeCandidateDto>(sql, new { req.Ids });
             return Ok(res);
        }

        public class GeocodeUpdateDto {
            public string Id { get; set; }
            public double Lat { get; set; }
            public double Lon { get; set; }
            public string Table { get; set; }
        }

        [HttpPost("geocode/update")]
        public async Task<IActionResult> UpdateGeocode([FromBody] List<GeocodeUpdateDto> updates)
        {
            if (updates == null || !updates.Any()) return Ok();

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var tran = conn.BeginTransaction();

            try {
                foreach(var u in updates) {
                    string table = u.Table;
                    // Whitelist check
                    if (!new []{"cadHandler", "DailyBulletinArrests", "sexoffender_registrants"}.Contains(table, StringComparer.OrdinalIgnoreCase)) continue;

                    string sql = $"UPDATE {table} SET lat=@Lat, lon=@Lon WHERE ";
                    if (table.Equals("cadHandler", StringComparison.OrdinalIgnoreCase)) 
                        sql += "id=@Id"; 
                    else if (table.Equals("DailyBulletinArrests", StringComparison.OrdinalIgnoreCase))
                        sql += "id=@Id";
                    else if (table.Equals("sexoffender_registrants", StringComparison.OrdinalIgnoreCase))
                        sql += "registrant_id=@Id";
                        
                    await conn.ExecuteAsync(sql, u, transaction: tran);
                }
                tran.Commit();
                return Ok(new { count = updates.Count });
            } catch (Exception ex) {
                tran.Rollback();
                return StatusCode(500, ex.Message);
            }
        }

        // --- DAB Time ---
        public class DabTimeCandidateDto {
            public string Id { get; set; }
            public string TimeText { get; set; }
        }

        [HttpGet("dab-time/candidates")]
        public async Task<IActionResult> GetDabTimeCandidates([FromQuery] int count = 100)
        {
            using var conn = new SqlConnection(_connectionString);
            // Script logic: (event_time IS NULL OR event_time = '1900-01-01')
            var sql = $"SELECT TOP {count} id as Id, [time] as TimeText FROM DailyBulletinArrests WHERE (event_time IS NULL OR event_time = '1900-01-01')";
            var res = await conn.QueryAsync<DabTimeCandidateDto>(sql);
            return Ok(res);
        }

        public class FetchDabTimeRequest {
            public List<string> Ids { get; set; }
        }

        [HttpPost("dab-time/fetch-details")]
        public async Task<IActionResult> FetchDabTimeDetails([FromBody] FetchDabTimeRequest req) {
             if (req.Ids == null || !req.Ids.Any()) return Ok(new List<DabTimeCandidateDto>());
             using var conn = new SqlConnection(_connectionString);
             var res = await conn.QueryAsync<DabTimeCandidateDto>("SELECT id as Id, [time] as TimeText FROM DailyBulletinArrests WHERE id IN @Ids", new { req.Ids });
             return Ok(res);
        }

        public class DabTimeUpdateDto {
            public string Id { get; set; }
            public DateTime EventTime { get; set; }
        }

        [HttpPost("dab-time/update")]
        public async Task<IActionResult> UpdateDabTime([FromBody] List<DabTimeUpdateDto> updates)
        {
            if (updates == null || !updates.Any()) return Ok();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var tran = conn.BeginTransaction();
            try {
                foreach(var u in updates) {
                    await conn.ExecuteAsync("UPDATE DailyBulletinArrests SET event_time=@EventTime WHERE id=@Id", u, transaction: tran);
                }
                tran.Commit();
                return Ok(new { count = updates.Count });
            } catch(Exception ex) {
                tran.Rollback();
                return StatusCode(500, ex.Message);
            }
        }
        public class DateRangeRequest {
            public DateTime Start { get; set; }
            public DateTime End { get; set; }
        }

        [HttpPost("daily-bulletin/ids")]
        public async Task<IActionResult> GetDailyBulletinIds([FromBody] DateRangeRequest req)
        {
            using var conn = new SqlConnection(_connectionString);
            var sql = "SELECT id FROM DailyBulletinArrests WHERE event_time >= @Start AND event_time <= @End";
            var res = await conn.QueryAsync<string>(sql, new { req.Start, req.End });
            return Ok(res);
        }
    }
}
