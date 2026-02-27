using Microsoft.AspNetCore.Mvc;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Text;
using Dapper;

namespace ScalableMssqlApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DataController : ControllerBase
    {
        private readonly IConfiguration _config;
        private readonly string _connectionString;
        private readonly ScalableMssqlApi.Services.Interfaces.IDataService _dataService;

        public DataController(IConfiguration config, ScalableMssqlApi.Services.Interfaces.IDataService dataService)
        {
            _config = config;
            _connectionString = _config.GetConnectionString("DefaultConnection") ?? _config["MssqlConnectionString"];
             if (string.IsNullOrEmpty(_connectionString)) _connectionString = config.GetConnectionString("Default"); // fallback

            _dataService = dataService;
        }


        [HttpGet("stats/probation")]
        public async Task<IActionResult> GetProbationStats()
        {
            var sql = @"
                SELECT
                  (SELECT COUNT(*) FROM dbo.Offender_Summary) as TotalOffenders,
                  
                  (SELECT SupervisionStatus as name, COUNT(*) as value 
                   FROM dbo.Offender_Charges 
                   WHERE SupervisionStatus IS NOT NULL 
                   GROUP BY SupervisionStatus 
                   FOR JSON PATH) as SupervisionDist,

                  (SELECT OffenseClass as name, COUNT(*) as value 
                   FROM dbo.Offender_Charges 
                   WHERE OffenseClass IS NOT NULL 
                   GROUP BY OffenseClass 
                   FOR JSON PATH) as ClassDist,

                  (SELECT Gender as name, COUNT(*) as value 
                   FROM dbo.Offender_Summary 
                   WHERE Gender IS NOT NULL 
                   GROUP BY Gender 
                   FOR JSON PATH) as GenderDist,

                  (SELECT AVG(CAST(Age as FLOAT)) FROM dbo.Offender_Summary WHERE Age > 0) as AvgAge,

                  (SELECT TOP 10 Offense as name, COUNT(*) as value 
                   FROM dbo.Offender_Detail 
                   WHERE Offense IS NOT NULL 
                   GROUP BY Offense 
                   ORDER BY COUNT(*) DESC 
                   FOR JSON PATH) as TopOffenses,

                  (SELECT TOP 10 CountyOfCommitment as name, COUNT(*) as value 
                   FROM dbo.Offender_Charges 
                   WHERE CountyOfCommitment IS NOT NULL 
                   GROUP BY CountyOfCommitment 
                   ORDER BY COUNT(*) DESC 
                   FOR JSON PATH) as CountyDist,

                  (SELECT TOP 12 FORMAT(CommitmentDate, 'yyyy-MM') as date, COUNT(*) as count
                   FROM dbo.Offender_Detail 
                   WHERE CommitmentDate IS NOT NULL 
                   GROUP BY FORMAT(CommitmentDate, 'yyyy-MM') 
                   ORDER BY date DESC 
                   FOR JSON PATH) as CommitmentTrend,

                  (SELECT TOP 10 S.Name, C.EndDate, D.Offense
                   FROM dbo.Offender_Charges C
                   JOIN dbo.Offender_Summary S ON C.OffenderNumber = S.OffenderNumber
                   LEFT JOIN dbo.Offender_Detail D ON C.OffenderNumber = D.OffenderNumber
                   WHERE C.EndDate BETWEEN GETDATE() AND DATEADD(day, 90, GETDATE())
                   ORDER BY C.EndDate ASC
                   FOR JSON PATH) as EndingSoon,

                  (SELECT TOP 10 S.Name, C.EndDate, D.Offense
                   FROM dbo.Offender_Charges C
                   JOIN dbo.Offender_Summary S ON C.OffenderNumber = S.OffenderNumber
                   LEFT JOIN dbo.Offender_Detail D ON C.OffenderNumber = D.OffenderNumber
                   WHERE C.EndDate > DATEADD(year, 1, GETDATE())
                   ORDER BY C.EndDate DESC
                   FOR JSON PATH) as LongestRemaining";
            
            return await ExecuteSafeQuery(sql);
        }

        [HttpGet("corrections")]
        public async Task<IActionResult> GetCorrections([FromQuery] int page = 1, [FromQuery] int limit = 50, [FromQuery] string? location = null, [FromQuery] string? search = null)
        {
            var offset = (page - 1) * limit;
            
            // 1. Fetch Summary IDs (Base Page)
            var baseSql = @"
                SELECT S.OffenderNumber, S.Name, S.Gender, S.Age, S.DateScraped
                FROM dbo.Offender_Summary S WITH (NOLOCK)
                ";
            
            bool needsDetailJoin = !string.IsNullOrEmpty(location) || !string.IsNullOrEmpty(search);

            if (needsDetailJoin)
            {
                baseSql += " JOIN dbo.Offender_Detail D WITH (NOLOCK) ON S.OffenderNumber = D.OffenderNumber WHERE 1=1";
            }
            else 
            {
                baseSql += " WHERE 1=1";
            }

            if (!string.IsNullOrEmpty(location))
            {
                baseSql += " AND D.Location = @Location";
            }

            if (!string.IsNullOrEmpty(search))
            {
                baseSql += " AND (S.Name LIKE @SearchPattern OR D.Offense LIKE @SearchPattern OR D.Location LIKE @SearchPattern)";
            }

            baseSql += @"
                ORDER BY S.DateScraped DESC
                OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY";

            var summaries = new List<Dictionary<string, object>>();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            
            // Fetch Base
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = baseSql;
                cmd.Parameters.AddWithValue("@Offset", offset);
                cmd.Parameters.AddWithValue("@Limit", limit);
                if (!string.IsNullOrEmpty(location))
                {
                    cmd.Parameters.AddWithValue("@Location", location);
                }
                if (!string.IsNullOrEmpty(search))
                {
                    cmd.Parameters.AddWithValue("@SearchPattern", $"%{search}%");
                }
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++) {
                        if (reader.IsDBNull(i))
                            row[reader.GetName(i)] = null;
                        else
                            row[reader.GetName(i)] = reader.GetValue(i);
                    }
                    summaries.Add(row);
                }
            }

            if (summaries.Count == 0) return Ok(new { data = summaries });

            // 2. Fetch Details for these IDs using simple string aggregation for IN clause (safe for ints/guids, sanitize strings)
            var ids = summaries.Select(s => s["OffenderNumber"].ToString()).Distinct().ToList();
            if (ids.Count == 0) return Ok(new { data = summaries });
            var idList = string.Join("','", ids.Select(id => id.Replace("'", "''"))); 
            
            var detailSql = $@"
                SELECT OffenderNumber, Location, Offense, CONVERT(varchar, CommitmentDate, 101) as CommitmentDate, CONVERT(varchar, TDD_SDD, 101) as TDD_SDD
                FROM dbo.Offender_Detail WITH (NOLOCK)
                WHERE OffenderNumber IN ('{idList}')";
            
            var chargeSql = $@"
                SELECT OffenderNumber, SupervisionStatus, OffenseClass, CountyOfCommitment, EndDate
                FROM dbo.Offender_Charges WITH (NOLOCK)
                WHERE OffenderNumber IN ('{idList}')";

            var details = new Dictionary<string, Dictionary<string, object>>();
            var charges = new Dictionary<string, List<Dictionary<string, object>>>();

            // Fetch Details
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = detailSql;
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var oid = reader["OffenderNumber"].ToString();
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++) {
                        if (reader.IsDBNull(i))
                            row[reader.GetName(i)] = null;
                        else
                            row[reader.GetName(i)] = reader.GetValue(i);
                    }
                    if (!details.ContainsKey(oid)) details[oid] = row;
                }
            }

            // Fetch Charges
            using (var cmd = conn.CreateCommand())
            {
               cmd.CommandText = chargeSql;
               using var reader = await cmd.ExecuteReaderAsync();
               while (await reader.ReadAsync())
               {
                   var oid = reader["OffenderNumber"].ToString();
                   var row = new Dictionary<string, object>();
                   for (int i = 0; i < reader.FieldCount; i++) {
                       if (reader.IsDBNull(i))
                           row[reader.GetName(i)] = null;
                       else
                           row[reader.GetName(i)] = reader.GetValue(i);
                   }
                   if (!charges.ContainsKey(oid)) charges[oid] = new List<Dictionary<string, object>>();
                   charges[oid].Add(row);
               }
            }

            // Merge
            foreach (var s in summaries)
            {
                var oid = s["OffenderNumber"].ToString();
                
                // Merge Detail
                if (details.TryGetValue(oid, out var d))
                {
                    s["Location"] = d.ContainsKey("Location") ? d["Location"] : null;
                    s["Offense"] = d.ContainsKey("Offense") ? d["Offense"] : null;
                    s["CommitmentDate"] = d.ContainsKey("CommitmentDate") ? d["CommitmentDate"] : null;
                    s["TDD_SDD"] = d.ContainsKey("TDD_SDD") ? d["TDD_SDD"] : null;
                }

                // Merge Charges (Aggregate)
                if (charges.TryGetValue(oid, out var cList))
                {
                   s["SupervisionStatus"] = string.Join(", ", cList.Select(c => $"{c["SupervisionStatus"]} ({c["OffenseClass"]})"));
                   s["OffenseClass"] = string.Join(", ", cList.Select(c => c["OffenseClass"]).Distinct());
                   s["CountyOfCommitment"] = cList.FirstOrDefault()?.GetValueOrDefault("CountyOfCommitment");
                   s["EndDate"] = cList.Max(c => c.ContainsKey("EndDate") ? c["EndDate"] as DateTime? : null);
                }
            }

            return Ok(new { data = summaries });
        }

        [HttpGet("dispatch")]
        public async Task<IActionResult> GetDispatch([FromQuery] int limit = 100, [FromQuery] DateTime? dateFrom = null, [FromQuery] DateTime? dateTo = null)
        {
            var sql = "SELECT TOP (@Limit) IncidentNumber, AgencyCode, NatureCode, TimeReceived, LocationAddress, LocationLat, LocationLong FROM dbo.DispatchCalls WHERE 1=1";
            var parameters = new Dictionary<string, object> { { "@Limit", limit } };

            if (dateFrom.HasValue) 
            {
                sql += " AND TimeReceived >= @DateFrom";
                parameters.Add("@DateFrom", dateFrom.Value);
            }
            if (dateTo.HasValue)
            {
                sql += " AND TimeReceived <= @DateTo";
                parameters.Add("@DateTo", dateTo.Value);
            }
            sql += " ORDER BY TimeReceived DESC";
            
            return await ExecuteSafeQuery(sql, parameters);
        }

        [HttpGet("corrections/recent")]
        public async Task<IActionResult> GetRecentDOC([FromQuery] string? search = null)
        {
            var sql = @"
                WITH RankedStatus AS (
                    SELECT 
                        [OffenderNumber],
                        [SupervisionStatus],
                        [CountyOfCommitment],
                        [EndDate],
                        LAG([SupervisionStatus]) OVER (PARTITION BY [OffenderNumber] ORDER BY ISNULL([EndDate], '9999-12-31') ASC) as PrevStatus,
                        ROW_NUMBER() OVER (PARTITION BY [OffenderNumber] ORDER BY ISNULL([EndDate], '9999-12-31') DESC) as RecencyRank
                    FROM [dbo].[Offender_Charges]
                )
                SELECT 
                    S.[Name],
                    R.[PrevStatus] + ' -> ' + R.[SupervisionStatus] as StatusUpdate,
                    D.[Offense],
                    S.[Gender],
                    S.[Age],
                    D.[Location],
                    CONVERT(varchar, D.[CommitmentDate], 101) as CommitmentDate,
                    CONVERT(varchar, D.[TDD_SDD], 101) as TDD_SDD,
                    R.[CountyOfCommitment],
                    R.[OffenderNumber]
                FROM RankedStatus R
                JOIN [dbo].[Offender_Detail] D ON R.[OffenderNumber] = D.[OffenderNumber]
                JOIN [dbo].[Offender_Summary] S ON R.[OffenderNumber] = S.[OffenderNumber]
                WHERE R.RecencyRank = 1 
                  AND R.PrevStatus IS NOT NULL 
                  AND R.PrevStatus <> R.[SupervisionStatus]
                  AND R.[CountyOfCommitment] = 'Dubuque'";

            var parameters = new Dictionary<string, object>();

            if (!string.IsNullOrEmpty(search))
            {
                sql += " AND (S.Name LIKE @SearchPattern OR D.Offense LIKE @SearchPattern OR D.Location LIKE @SearchPattern)";
                parameters.Add("@SearchPattern", $"%{search}%");
            }

            sql += " ORDER BY D.[CommitmentDate] DESC";
            
            return await ExecuteSafeQuery(sql, parameters);
        }

        [HttpGet("corrections/locations")]
        public async Task<IActionResult> GetDOCLocations()
        {
            var sql = "SELECT DISTINCT Location FROM dbo.Offender_Detail WITH (NOLOCK) WHERE Location IS NOT NULL AND Location != '' ORDER BY Location";
            return await ExecuteSafeQuery(sql, new Dictionary<string, object>());
        }

        [HttpGet("traffic")]
        public async Task<IActionResult> GetTraffic([FromQuery] int limit = 100, [FromQuery] DateTime? dateFrom = null, [FromQuery] DateTime? dateTo = null, [FromQuery] string? search = null)
        {
             var sql = "SELECT TOP (@Limit) [key], event_time, charge, name, location, row_hash as event_number, TRY_CAST(lat as float) as lat, TRY_CAST(lon as float) as lon FROM dbo.DailyBulletinArrests WITH (NOLOCK) WHERE ([key] != 'AR' AND [key] != 'LW')";
             var parameters = new Dictionary<string, object> { { "@Limit", limit } };

             if (dateFrom.HasValue) 
             {
                 sql += " AND event_time >= @DateFrom";
                 parameters.Add("@DateFrom", dateFrom.Value);
             }
             if (dateTo.HasValue)
             {
                 sql += " AND event_time <= @DateTo";
                 parameters.Add("@DateTo", dateTo.Value);
             }
             if (!string.IsNullOrWhiteSpace(search))
             {
                 sql += " AND FREETEXT(*, @Search)";
                 parameters.Add("@Search", search);
             }
             sql += " ORDER BY event_time DESC";

             return await ExecuteSafeQuery(sql, parameters);
        }

        [HttpGet("stats/database")]
        public async Task<IActionResult> GetDatabaseStats()
        {
            // Combined Query for efficiency
            var sql = @"
                SELECT SUM(size) * 8 / 1024 AS SizeMB FROM sys.database_files;
                SELECT MIN(starttime) as val FROM cadHandler;
                SELECT MIN(event_time) as val FROM DailyBulletinArrests;
                SELECT COUNT(*) as Total, COUNT(lat) as WithGeo FROM cadHandler;
                SELECT COUNT(*) as Total, COUNT(lat) as WithGeo FROM DailyBulletinArrests WHERE [key] = 'AR';
            ";
            
            var results = new Dictionary<string, object>();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;

            using var reader = await cmd.ExecuteReaderAsync();
            
            // 1. Size
            if (await reader.ReadAsync()) results["sizeMB"] = reader.GetValue(0);
            
            // 2. Oldest CAD
            await reader.NextResultAsync();
            if (await reader.ReadAsync()) results["oldestCad"] = reader.GetValue(0);

            // 3. Oldest Arrest
            await reader.NextResultAsync();
            if (await reader.ReadAsync()) results["oldestArrest"] = reader.GetValue(0);

             // 4. Geo CAD
            await reader.NextResultAsync();
            var cadTotal = 0; var cadGeo = 0;
            if (await reader.ReadAsync()) { cadTotal = Convert.ToInt32(reader["Total"]); cadGeo = Convert.ToInt32(reader["WithGeo"]); }

            // 5. Geo Arrest
            await reader.NextResultAsync();
            var arrTotal = 0; var arrGeo = 0;
            if (await reader.ReadAsync()) { arrTotal = Convert.ToInt32(reader["Total"]); arrGeo = Convert.ToInt32(reader["WithGeo"]); }
            
            var total = cadTotal + arrTotal;
            var totalGeo = cadGeo + arrGeo;
            
            results["geocoding"] = new {
                cad = new { Total = cadTotal, WithGeo = cadGeo },
                arrest = new { Total = arrTotal, WithGeo = arrGeo },
                totalRate = total > 0 ? ((double)totalGeo / total * 100).ToString("F1") : "0.0"
            };

            return Ok(new { data = results });
        }


        // --- Helper for boilerplate execution ---
        private async Task<IActionResult> ExecuteSafeQuery(string sql, Dictionary<string, object>? parameters = null) {
             var results = new List<Dictionary<string, object>>();
             using var conn = new SqlConnection(_connectionString);
             await conn.OpenAsync();
             using var cmd = conn.CreateCommand();
             cmd.CommandText = sql;
             cmd.CommandTimeout = 120; // Increase timeout for slow queries
             if (parameters != null) {
                 foreach (var kvp in parameters) cmd.Parameters.AddWithValue(kvp.Key, kvp.Value ?? DBNull.Value);
             }
             using var reader = await cmd.ExecuteReaderAsync();
             while (await reader.ReadAsync()) {
                 var row = new Dictionary<string, object>();
                 for (int i = 0; i < reader.FieldCount; i++) {
                     if (reader.IsDBNull(i)) 
                         row[reader.GetName(i)] = null;
                     else if (reader.GetName(i) == "photo_data" && reader.GetFieldType(i) == typeof(byte[]))
                         row[reader.GetName(i)] = Convert.ToBase64String((byte[])reader.GetValue(i));
                     else 
                         row[reader.GetName(i)] = reader.GetValue(i);
                 }
                 results.Add(row); 
             }
             return Ok(new { data = results });
        }

        [HttpGet("sex-offenders")]
        public async Task<IActionResult> GetSexOffenders([FromQuery] int page = 1, [FromQuery] int limit = 50, [FromQuery] string? search = null)
        {
            var offset = (page - 1) * limit;
            var sql = @"
                SELECT 
                  R.registrant_id,
                  R.first_name,
                  R.middle_name,
                  R.last_name,
                  R.address_line_1,
                  R.city,
                  R.state,
                  R.postal_code,
                  R.county,
                  R.lat,
                  R.lon,
                  R.photo_url,
                  R.tier,
                  R.gender,
                  R.race,
                  R.victim_minors,
                  R.victim_adults,
                  R.victim_unknown,
                  R.registrant_cluster,
                  CONVERT(varchar, R.last_changed, 101) as last_changed,
                  (
                    SELECT STRING_AGG(V.gender + ' (' + V.age_group + ')', ', ')
                    FROM sexoffender_convictions C
                    JOIN sexoffender_conviction_victims V ON C.conviction_id = V.conviction_id
                    WHERE C.registrant_id = R.registrant_id
                  ) as victim_info
                FROM dbo.sexoffender_registrants R
                WHERE 1=1";

            var parameters = new Dictionary<string, object> { { "@Offset", offset }, { "@Limit", limit } };

            if (!string.IsNullOrWhiteSpace(search))
            {
                sql += " AND (R.first_name LIKE @Search OR R.last_name LIKE @Search OR R.address_line_1 LIKE @Search OR R.city LIKE @Search OR R.county LIKE @Search)";
                parameters.Add("@Search", "%" + search + "%");
            }

            sql += @"
                ORDER BY R.last_changed DESC
                OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY";

            var results = new List<Dictionary<string, object>>();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            foreach (var p in parameters) cmd.Parameters.AddWithValue(p.Key, p.Value);

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
            if (string.IsNullOrWhiteSpace(q) || q.Length < 2)
                return BadRequest("Invalid search query.");

            var offset = (page - 1) * limit;
            var term = "%" + q + "%";

            // Unified Person Search Query - Parameterized
            var sql = @"
                SELECT 'ARREST' as type, event_time as date, name, charge as details, location, row_hash as ref_id, NULL as lat, NULL as lon FROM dbo.DailyBulletinArrests WHERE name LIKE @Term
                UNION ALL
                SELECT 'CITATION' as type, event_time as date, name, charge as details, location, row_hash as ref_id, NULL, NULL FROM dbo.DailyBulletinArrests WHERE [key] = 'TC' AND name LIKE @Term
                UNION ALL
                SELECT 'ACCIDENT' as type, event_time as date, name, charge as details, location, row_hash as ref_id, NULL, NULL FROM dbo.DailyBulletinArrests WHERE [key] = 'TA' AND name LIKE @Term
                UNION ALL
                SELECT 'CRIME' as type, event_time as date, name, charge as details, location, row_hash as ref_id, NULL, NULL FROM dbo.DailyBulletinArrests WHERE [key] = 'LW' AND name LIKE @Term
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
        public async Task<IActionResult> GetIncidents(
            [FromQuery] string? search = null,
            [FromQuery] string? types = null,
            [FromQuery] int page = 1,
            [FromQuery] int limit = 50,
            [FromQuery] DateTime? dateFrom = null,
            [FromQuery] DateTime? dateTo = null,
            [FromQuery] double? lat = null,
            [FromQuery] double? lon = null,
            [FromQuery] double radiusMiles = 1.0)
        {
             var offset = (page - 1) * limit;
             var results = new List<Dictionary<string, object>>();
             using var conn = new SqlConnection(_connectionString);
             await conn.OpenAsync();
             using var cmd = conn.CreateCommand();

             var whereArr = "1=1";
             var whereCad = "1=1";
             var whereSex = "1=1";
             var parameters = new Dictionary<string, object> { 
                 { "@Limit", limit }, 
                 { "@Offset", offset } 
             };

             // Type Filtering
             // Types: arrest, crime, traffic, accident, cad, sex_offender
             // If types are provided, we narrow down the 1=1 default.
             // If a subquery is NOT needed, we can set its where to 1=0 to skip it efficiently? 
             // Or better, we build the UNION dynamically or just filter inside each. 
             // Filtering inside is safer and easier.
             
             bool includeArrest = true; // AR
             bool includeCrime = true;  // LW
             bool includeTraffic = true; // TC
             bool includeAccident = true; // TA
             bool includeCad = true;
             bool includeSex = true;

             if (!string.IsNullOrWhiteSpace(types)) {
                 var t = types.ToLower().Split(',', StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToHashSet();
                 if (t.Any()) {
                     includeArrest = t.Contains("arrest");
                     includeCrime = t.Contains("crime");
                     includeTraffic = t.Contains("traffic");
                     includeAccident = t.Contains("accident");
                     includeCad = t.Contains("cad") || t.Contains("details"); // 'details' legacy?
                     includeSex = t.Contains("sex_offender");
                 }
             }

             // Apply filters to DailyBulletinArrests based on keys
             // [key] IN (...)
             var arrestKeys = new List<string>();
             if (includeArrest) arrestKeys.Add("'AR'");
             if (includeCrime) arrestKeys.Add("'LW'");
             if (includeTraffic) arrestKeys.Add("'TC'");
             if (includeAccident) arrestKeys.Add("'TA'");
             
             if (arrestKeys.Any()) {
                 whereArr += $" AND [key] IN ({string.Join(",", arrestKeys)})";
             } else {
                 whereArr = "1=0"; // Skip this table
             }
             
             if (!includeCad) whereCad = "1=0";
             if (!includeSex) whereSex = "1=0";

             if (dateFrom.HasValue) {
                 whereArr += " AND event_time >= @DateFrom";
                 whereCad += " AND starttime >= @DateFrom";
                 // Sex offenders dont have event time, maybe date_scraped or last_changed? 
                 // Let's use date_scraped or valid_date. If null, include? 
                 // Usually sex offenders are evergreen list, so maybe ignore date filter? 
                 // User wants a feed. If date filter is applied, maybe just omit?
                 // Or map 'last_update_date' if available. 
                 // Sex offenders
                 // Use last_changed as event time proxy
                 whereSex += " AND last_changed >= @DateFrom";
                 parameters.Add("@DateFrom", dateFrom.Value);
             }
             if (dateTo.HasValue) {
                 whereArr += " AND event_time <= @DateTo";
                 whereCad += " AND starttime <= @DateTo";
                 whereSex += " AND last_changed <= @DateTo";
                 parameters.Add("@DateTo", dateTo.Value);
             }

             if (!string.IsNullOrWhiteSpace(search)) {
                 var term = search;
                 // FREETEXT(*, @Search) natively interrogates all Full-Text Indexed columns in the tables 
                 // parsing semantic stems, plurals, and fuzzy concepts instantly without scanning.
                 whereArr += " AND FREETEXT(*, @Search)";
                 whereCad += " AND FREETEXT(*, @Search)";
                 whereSex += " AND FREETEXT(*, @Search)";
                 parameters.Add("@Search", term);
             }

             // Bounding Box Spatial Filter (~69.1 miles per degree latitude)
             if (lat.HasValue && lon.HasValue) {
                 double milesPerDegreeLat = 69.1;
                 double milesPerDegreeLon = milesPerDegreeLat * Math.Cos(lat.Value * Math.PI / 180.0);
                 if (milesPerDegreeLon < 0.01) milesPerDegreeLon = 0.01; // Avoid divide by zero
                 
                 double deltaLat = radiusMiles / milesPerDegreeLat;
                 double deltaLon = radiusMiles / milesPerDegreeLon;

                 whereArr += " AND TRY_CAST(lat as float) BETWEEN @MinLat AND @MaxLat AND TRY_CAST(lon as float) BETWEEN @MinLon AND @MaxLon";
                 whereCad += " AND TRY_CAST(lat as float) BETWEEN @MinLat AND @MaxLat AND TRY_CAST(lon as float) BETWEEN @MinLon AND @MaxLon";
                 whereSex += " AND TRY_CAST(lat as float) BETWEEN @MinLat AND @MaxLat AND TRY_CAST(lon as float) BETWEEN @MinLon AND @MaxLon";

                 parameters.Add("@MinLat", lat.Value - deltaLat);
                 parameters.Add("@MaxLat", lat.Value + deltaLat);
                 parameters.Add("@MinLon", lon.Value - deltaLon);
                 parameters.Add("@MaxLon", lon.Value + deltaLon);
             }

             // Unified Query
             // Aliases: ID, EventTime, Description, Location, PrimaryPerson, SourceType, Lat, Lon

             var sql = $@"
                 SELECT ID, EventTime, CloseTime, Description, Location, PrimaryPerson, SourceType, Lat, Lon 
                 FROM (
                     SELECT 
                        CAST(row_hash as varchar(128)) as ID, 
                        event_time as EventTime, 
                        NULL as CloseTime,
                        charge as Description, 
                        location as Location, 
                        name as PrimaryPerson, 
                        [key] as SourceType, 
                        TRY_CAST(lat as float) as Lat, 
                        TRY_CAST(lon as float) as Lon
                     FROM dbo.DailyBulletinArrests WITH (NOLOCK)
                     WHERE {whereArr}
                     
                     UNION ALL

                     SELECT 
                        CAST(id as varchar(50)) as ID, 
                        starttime as EventTime, 
                        closetime as CloseTime,
                        nature as Description, 
                        address as Location, 
                        agency as PrimaryPerson, 
                        'CAD' as SourceType, 
                        TRY_CAST(lat as float) as Lat, 
                        TRY_CAST(lon as float) as Lon
                     FROM dbo.cadHandler WITH (NOLOCK)
                     WHERE {whereCad}

                     UNION ALL
                     
                     SELECT 
                         CAST(registrant_id as varchar(50)) as ID,
                         last_changed as EventTime,
                         NULL as CloseTime,
                         'Registered Sex Offender' as Description,
                         ISNULL(address_line_1, '') + ' ' + ISNULL(city, '') as Location,
                         first_name + ' ' + last_name as PrimaryPerson,
                         'SEX_OFFENDER' as SourceType,
                         TRY_CAST(lat as float) as Lat,
                         TRY_CAST(lon as float) as Lon
                     FROM dbo.sexoffender_registrants WITH (NOLOCK)
                     WHERE {whereSex}

                 ) AS Combined
                 ORDER BY EventTime DESC
                 OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY";

             cmd.CommandText = sql;
             foreach(var p in parameters) cmd.Parameters.AddWithValue(p.Key, p.Value ?? DBNull.Value);

             using var reader = await cmd.ExecuteReaderAsync();
             while (await reader.ReadAsync()) {
                 var row = new Dictionary<string, object>();
                 for (int i = 0; i < reader.FieldCount; i++) {
                     if (reader.IsDBNull(i)) 
                         row[reader.GetName(i)] = null;
                     else 
                         row[reader.GetName(i)] = reader.GetValue(i);
                 }
                 results.Add(row); 
             }
             
             // Meta for pagination
             return Ok(new { data = results, meta = new { page, limit } });
        }

        [HttpGet("stats/jail")]
        public async Task<IActionResult> GetJailStats()
        {
             // 1. Base Stats
             // 1. Base Stats - Consolidated into one query with NOLOCK
             var baseSql = @"
                SELECT
                  COUNT(*) as TotalInmates,
                  AVG(TRY_CAST(replace(total_bond_amount, '$', '') AS float)) as AvgBond,
                  SUM(TRY_CAST(replace(total_bond_amount, '$', '') AS float)) as TotalBond
                FROM jail_inmates WITH (NOLOCK)
                WHERE released_date IS NULL;

                SELECT sex as name, COUNT(*) as value 
                FROM jail_inmates WITH (NOLOCK)
                WHERE released_date IS NULL AND sex IS NOT NULL 
                GROUP BY sex FOR JSON PATH;

                SELECT race as name, COUNT(*) as value 
                FROM jail_inmates WITH (NOLOCK)
                WHERE released_date IS NULL AND race IS NOT NULL 
                GROUP BY race FOR JSON PATH;";
             
             // 2. Charge Counts - Added NOLOCK
             var chargeSql = @"
                SELECT TOP 10 charge_description as name, COUNT(*) as value
                FROM jail_charges c WITH (NOLOCK)
                JOIN jail_inmates i WITH (NOLOCK) ON c.book_id = i.book_id
                WHERE i.released_date IS NULL AND charge_description IS NOT NULL
                GROUP BY charge_description
                ORDER BY COUNT(*) DESC
                FOR JSON PATH";

             var results = new Dictionary<string, object>();
             using var conn = new SqlConnection(_connectionString);
             await conn.OpenAsync();
             using var cmd = conn.CreateCommand();
             cmd.CommandText = baseSql + "; " + chargeSql;
             cmd.CommandTimeout = 120; // Increase timeout

             using var reader = await cmd.ExecuteReaderAsync();
             
             // 1. Aggregates
             if (await reader.ReadAsync()) {
                 results["TotalInmates"] = reader["TotalInmates"];
                 results["AvgBond"] = reader["AvgBond"];
                 results["TotalBond"] = reader["TotalBond"];
             }

             // 2. SexDist
             if (await reader.NextResultAsync()) {
                 if (await reader.ReadAsync()) results["SexDist"] = reader.GetValue(0);
             }

             // 3. RaceDist
             if (await reader.NextResultAsync()) {
                 if (await reader.ReadAsync()) results["RaceDist"] = reader.GetValue(0);
             }

             // 4. TopCharges
             if (await reader.NextResultAsync()) {
                 if (await reader.ReadAsync()) results["TopCharges"] = reader.GetValue(0);
             }

             return Ok(new { data = new[] { results } });
        }

        [HttpGet("jail/inmates")]
        public async Task<IActionResult> GetJailInmates([FromQuery] int page = 1, [FromQuery] int limit = 1000, [FromQuery] bool activeOnly = false, [FromQuery] string? search = null)
        {
            var offset = (page - 1) * limit;
            var where = "1=1";
            var parameters = new Dictionary<string, object> { { "@Limit", limit }, { "@Offset", offset } };

            if (activeOnly) where += " AND released_date IS NULL";
            if (!string.IsNullOrWhiteSpace(search)) {
                where += " AND (lastname LIKE @Search OR firstname LIKE @Search OR invid LIKE @Search)";
                parameters.Add("@Search", "%" + search + "%");
            }

            // 1. Fetch Inmates
            var sql = $@"
                SELECT 
                  book_id, invid, firstname, lastname, dob, arrest_date, released_date,
                  age, race, sex, total_bond_amount, next_court_date
                FROM jail_inmates WITH (NOLOCK)
                WHERE {where}
                ORDER BY arrest_date DESC
                OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY";

            var inmates = new List<Dictionary<string, object>>();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandTimeout = 120; // 2 min timeout
                foreach(var p in parameters) cmd.Parameters.AddWithValue(p.Key, p.Value);

                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++) {
                        if (reader.IsDBNull(i))
                            row[reader.GetName(i)] = null;
                        else
                            row[reader.GetName(i)] = reader.GetValue(i);
                    }
                    inmates.Add(row);
                }
            }

            if (inmates.Count == 0) return Ok(new { data = inmates });

            // 2. Fetch Charges for these BookIDs
            var bookIds = inmates.Select(i => i["book_id"].ToString()).Distinct().ToList();
            var idList = string.Join("','", bookIds.Select(id => id.Replace("'", "''"))); // Safety

            var chargesSql = $@"
                SELECT book_id, charge_description 
                FROM jail_charges WITH (NOLOCK)
                WHERE book_id IN ('{idList}')
                ORDER BY charge_description"; // Order for consistency

            var charges = new Dictionary<string, List<string>>();
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = chargesSql;
                cmd.CommandTimeout = 120;
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                   var bid = reader["book_id"].ToString();
                   var desc = reader["charge_description"]?.ToString();
                   if (!string.IsNullOrEmpty(desc))
                   {
                       if (!charges.ContainsKey(bid)) charges[bid] = new List<string>();
                       charges[bid].Add(desc);
                   }
                }
            }

            // 3. Merge
            foreach (var img in inmates)
            {
                var bid = img["book_id"].ToString();
                if (charges.TryGetValue(bid, out var cList))
                {
                    img["charges"] = string.Join(", ", cList);
                }
                else
                {
                    img["charges"] = "";
                }
            }

            return Ok(new { data = inmates });
        }

        [HttpGet("jail/inmates/{bookId}/image")]
        public async Task<IActionResult> GetJailImage(string bookId)
        {
            var sql = "SELECT photo_data FROM jail_photos WHERE book_id = @BookId";
            var result = await ExecuteSafeQuery(sql, new Dictionary<string, object> { { "@BookId", bookId } });
            // ExecuteSafeQuery returns a list of dictionaries. For image, clearer to return just the object?
            // The client expects rawQuery format: { response: { data: [ { photo_data: ... } ] } }
            // API returns { data: [ ... ] } which matches.
            return result;
        }

        [HttpGet("jail/history")]
        public async Task<IActionResult> GetInmateHistory([FromQuery] string lastname, [FromQuery] string firstname, [FromQuery] string? dob = null)
        {
            if (string.IsNullOrWhiteSpace(lastname) || string.IsNullOrWhiteSpace(firstname)) return BadRequest("Name required");
            
            var sql = @"
                SELECT 
                  book_id, invid, firstname, lastname, middlename, disp_name,
                  age, dob, sex, race, arrest_date, agency, disp_agency,
                  total_bond_amount, next_court_date, released_date,
                  (SELECT COUNT(*) FROM jail_charges WHERE book_id = i.book_id) as charge_count,
                  (SELECT STRING_AGG(charge_description, ', ') WITHIN GROUP (ORDER BY charge_description) FROM jail_charges WHERE book_id = i.book_id) as charges
                FROM jail_inmates i
                WHERE lastname = @Last AND firstname = @First";
            
            var parameters = new Dictionary<string, object> { { "@Last", lastname }, { "@First", firstname } };
            if (!string.IsNullOrWhiteSpace(dob)) {
                sql += " AND dob = @Dob";
                parameters.Add("@Dob", dob);
            }
            sql += " ORDER BY arrest_date DESC";

            return await ExecuteSafeQuery(sql, parameters);
        }
        
        [HttpGet("search/person")]
        public async Task<IActionResult> SearchPerson([FromQuery] string q, [FromQuery] string type = "jail")
        {
             if (string.IsNullOrWhiteSpace(q)) return BadRequest("Query required");
             var terms = q.Split(' ', StringSplitOptions.RemoveEmptyEntries);
             var paramsDict = new Dictionary<string, object>();
             
             // Simple first/last name logic
             string where;
             if (terms.Length > 1) {
                 where = "(firstname LIKE @First AND lastname LIKE @Last)";
                 paramsDict.Add("@First", "%" + terms[0] + "%");
                 paramsDict.Add("@Last", "%" + terms[1] + "%");
             } else {
                 where = "(firstname LIKE @Q OR lastname LIKE @Q)";
                 paramsDict.Add("@Q", "%" + q + "%");
             }

             string sql;
             if (type == "jail") {
                 sql = $@"SELECT i.book_id, i.firstname, i.lastname, i.arrest_date, i.released_date, i.age, i.race, i.sex, i.total_bond_amount,
                          (SELECT STRING_AGG(charge_description, ', ') FROM jail_charges WHERE book_id = i.book_id) as charges
                          FROM jail_inmates i WHERE {where} ORDER BY i.arrest_date DESC";
             } 
             else if (type == "arrest") {
                 sql = $"SELECT TOP 20 row_hash as id, event_time, charge, name, firstname, lastname, location, [key] FROM dbo.DailyBulletinArrests WHERE {where} AND [key] = 'AR' ORDER BY event_time DESC";
             }
             else if (type == "traffic") {
                 sql = $"SELECT TOP 20 row_hash as id, event_time, charge, name, firstname, lastname, location, [key] FROM dbo.DailyBulletinArrests WHERE {where} AND ([key] = 'TC' OR [key] = 'TA') ORDER BY event_time DESC";
             }
             else if (type == "crime") {
                 sql = $"SELECT TOP 20 row_hash as id, event_time, charge, name, firstname, lastname, location, [key] FROM dbo.DailyBulletinArrests WHERE {where} AND [key] = 'LW' ORDER BY event_time DESC";
             }
             else return BadRequest("Invalid type");

             return await ExecuteSafeQuery(sql, paramsDict);
        }

        [HttpGet("offenders/{number}")]
        public async Task<IActionResult> GetOffenderDetail(string number)
        {
             var finalRes = new Dictionary<string, object>();
             using var conn = new SqlConnection(_connectionString);
             await conn.OpenAsync();
             using var cmd = conn.CreateCommand();
             cmd.Parameters.AddWithValue("@Num", number);

             // 1. Summary
             cmd.CommandText = "SELECT TOP 1 OffenderNumber, Name, Gender, Age, DateScraped FROM dbo.Offender_Summary WHERE OffenderNumber = @Num";
             using (var r = await cmd.ExecuteReaderAsync()) {
                  if (await r.ReadAsync()) {
                      var row = new Dictionary<string, object>();
                      for(int i=0; i<r.FieldCount; i++) {
                           if (r.IsDBNull(i)) row[r.GetName(i)] = null;
                           else row[r.GetName(i)] = r.GetValue(i);
                      }
                      finalRes["summary"] = row;
                  }
             }
             
             // 2. Detail
             cmd.CommandText = "SELECT TOP 1 OffenderNumber, Location, Offense, TDD_SDD, CommitmentDate, RecallDate, InterviewDate, MandatoryMinimum, DecisionType, Decision, DecisionDate, EffectiveDate FROM dbo.Offender_Detail WHERE OffenderNumber = @Num";
             using (var r = await cmd.ExecuteReaderAsync()) {
                  if (await r.ReadAsync()) {
                      var row = new Dictionary<string, object>();
                      for(int i=0; i<r.FieldCount; i++) {
                           if (r.IsDBNull(i)) row[r.GetName(i)] = null;
                           else row[r.GetName(i)] = r.GetValue(i);
                      }
                      finalRes["detail"] = row;
                  }
             }

             // 3. Charges
             cmd.CommandText = "SELECT ChargeID, OffenderNumber, SupervisionStatus, OffenseClass, CountyOfCommitment, EndDate FROM dbo.Offender_Charges WHERE OffenderNumber = @Num ORDER BY EndDate DESC";
             var chargesList = new List<Dictionary<string, object>>();
             using (var r = await cmd.ExecuteReaderAsync()) {
                  while (await r.ReadAsync()) {
                      var row = new Dictionary<string, object>();
                      for(int i=0; i<r.FieldCount; i++) {
                           if (r.IsDBNull(i)) row[r.GetName(i)] = null;
                           else row[r.GetName(i)] = r.GetValue(i);
                      }
                      chargesList.Add(row);
                  }
             }
             finalRes["charges"] = chargesList;

             return Ok(new { data = finalRes });
        }

        [HttpGet("reoffenders")]
        public async Task<IActionResult> GetReoffenders([FromQuery] int limit = 50, [FromQuery] int page = 1, [FromQuery] DateTime? dateFrom = null, [FromQuery] DateTime? dateTo = null)
        {
            var offset = (page - 1) * limit;
            var sql = @"
                SELECT A.ArrestRecordName as name, A.ArrestCharge as details, A.OriginalOffenses, A.DocOffenderNumber as OffenderNumbers, 
                       A.ArrestDate as event_time, A.ArrestDate as date, A.ArrestLocation as location, A.JailBondAmount, A.TDD_SDD,
                       JP.photo_data
                FROM vw_ViolatorsWithJailInfo A
                OUTER APPLY (
                    SELECT TOP 1 p.photo_data
                    FROM jail_inmates i WITH (NOLOCK)
                    JOIN jail_photos p WITH (NOLOCK) ON p.book_id = i.book_id
                    WHERE UPPER(i.lastname) = UPPER(LTRIM(RTRIM(LEFT(A.ArrestRecordName, CHARINDEX(',', A.ArrestRecordName + ',') - 1))))
                      AND UPPER(i.firstname) LIKE UPPER(LTRIM(RTRIM(
                            LEFT(
                                SUBSTRING(A.ArrestRecordName, CHARINDEX(',', A.ArrestRecordName) + 2, 100),
                                CASE 
                                    WHEN CHARINDEX(' ', LTRIM(SUBSTRING(A.ArrestRecordName, CHARINDEX(',', A.ArrestRecordName) + 2, 100))) > 0
                                    THEN CHARINDEX(' ', LTRIM(SUBSTRING(A.ArrestRecordName, CHARINDEX(',', A.ArrestRecordName) + 2, 100))) - 1
                                    ELSE LEN(SUBSTRING(A.ArrestRecordName, CHARINDEX(',', A.ArrestRecordName) + 2, 100))
                                END
                            )
                          ))) + '%'
                      AND p.photo_data IS NOT NULL
                    ORDER BY i.arrest_date DESC
                ) JP
                WHERE 1=1";
            
            var parameters = new Dictionary<string, object> { { "@Limit", limit }, { "@Offset", offset } };
            if (dateFrom.HasValue) { sql += " AND A.ArrestDate >= @DateFrom"; parameters.Add("@DateFrom", dateFrom.Value); }
            if (dateTo.HasValue) { sql += " AND A.ArrestDate <= @DateTo"; parameters.Add("@DateTo", dateTo.Value); }
            
            sql += " ORDER BY A.ArrestDate DESC OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY";
            return await ExecuteSafeQuery(sql, parameters);
        }

        [HttpGet("proximity")]
        public async Task<IActionResult> GetProximity([FromQuery] string address, [FromQuery] double? lat = null, [FromQuery] double? lon = null, [FromQuery] int days = 7, [FromQuery] int distance = 1000, [FromQuery] string? nature = null)
        {
             if (!lat.HasValue || !lon.HasValue) return BadRequest("Lat/Lon required for proximity search (geocoding must be done by caller)");

             var sql = $@"
               SELECT TOP 50 id, starttime, closetime, agency, service, nature, address, geox, geoy,
                 SQRT(POWER(CAST(geox AS FLOAT) - @X, 2) + POWER(CAST(geoy AS FLOAT) - @Y, 2)) AS distance_ft
               FROM cadHandler
               WHERE starttime >= @StartDate AND SQRT(POWER(CAST(geox AS FLOAT) - @X, 2) + POWER(CAST(geoy AS FLOAT) - @Y, 2)) <= @Dist";
             
             if (!string.IsNullOrEmpty(nature)) sql += " AND nature LIKE @Nature";
             sql += " ORDER BY starttime DESC, distance_ft ASC";

             var parameters = new Dictionary<string, object> { 
                 { "@X", lat.Value },
                 { "@Y", lon.Value },
                 { "@Dist", distance },
                 { "@StartDate", DateTime.Now.AddDays(-days) }
             };
             if (!string.IsNullOrEmpty(nature)) parameters.Add("@Nature", "%" + nature + "%");

             return await ExecuteSafeQuery(sql, parameters);
        }

        [HttpGet("premise-history")]
        public async Task<IActionResult> GetPremiseHistory([FromQuery] string address)
        {
             var sql = "SELECT TOP 50 id, event_time, charge, name, location, [key], SourceType FROM vw_AllEvents WHERE Location = @Address ORDER BY event_time DESC";
             return await ExecuteSafeQuery(sql, new Dictionary<string, object> { { "@Address", address } });
        }


        // --- Helper for boilerplate execution ---

        [HttpGet("query")]
        public async Task<IActionResult> Query(
            [FromQuery] string table,
            [FromQuery] string? columns = null,
            [FromQuery] int limit = 100,
            [FromQuery] string? filters = null,
            [FromQuery] string? orderBy = null)
        {
             try 
             {
                 var results = await _dataService.QueryAsync(table, columns, filters, orderBy, limit, null);
                 return Ok(new
                 {
                     success = true,
                     data = results
                 });
             }
             catch (ArgumentException ex) {
                 return BadRequest(ex.Message);
             }
             catch (Exception ex) {
                 return StatusCode(500, "Internal Server Error");
             }
        }

        // Endpoints removed for security hardening: GetTables, GetSchema, Insert.
    }
}
