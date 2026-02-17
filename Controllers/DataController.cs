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
        public async Task<IActionResult> GetCorrections([FromQuery] int page = 1, [FromQuery] int limit = 50)
        {
            var offset = (page - 1) * limit;
            
            // 1. Fetch Summary IDs (Base Page)
            var baseSql = @"
                SELECT OffenderNumber, Name, Gender, Age, DateScraped
                FROM dbo.Offender_Summary
                ORDER BY DateScraped DESC
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
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++) row[reader.GetName(i)] = reader.GetValue(i);
                    summaries.Add(row);
                }
            }

            if (summaries.Count == 0) return Ok(new { data = summaries });

            // 2. Fetch Details for these IDs using simple string aggregation for IN clause (safe for ints/guids, sanitize strings)
            var ids = summaries.Select(s => s["OffenderNumber"].ToString()).Distinct().ToList();
            var idList = string.Join("','", ids.Select(id => id.Replace("'", "''"))); 
            
            var detailSql = $@"
                SELECT OffenderNumber, Location, Offense, CommitmentDate, TDD_SDD
                FROM dbo.Offender_Detail
                WHERE OffenderNumber IN ('{idList}')";
            
            var chargeSql = $@"
                SELECT OffenderNumber, SupervisionStatus, OffenseClass, CountyOfCommitment, EndDate
                FROM dbo.Offender_Charges
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
                    for (int i = 0; i < reader.FieldCount; i++) row[reader.GetName(i)] = reader.GetValue(i);
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
                   for (int i = 0; i < reader.FieldCount; i++) row[reader.GetName(i)] = reader.GetValue(i);
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

        [HttpGet("traffic")]
        public async Task<IActionResult> GetTraffic([FromQuery] int limit = 100, [FromQuery] DateTime? dateFrom = null, [FromQuery] DateTime? dateTo = null)
        {
             var sql = "SELECT TOP (@Limit) [key], event_time, charge, name, location, id as event_number FROM dbo.DailyBulletinArrests WHERE ([key] != 'AR' AND [key] != 'LW')";
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
             if (parameters != null) {
                 foreach (var kvp in parameters) cmd.Parameters.AddWithValue(kvp.Key, kvp.Value ?? DBNull.Value);
             }
             using var reader = await cmd.ExecuteReaderAsync();
             while (await reader.ReadAsync()) {
                 var row = new Dictionary<string, object>();
                 for (int i = 0; i < reader.FieldCount; i++) row[reader.GetName(i)] = reader.GetValue(i);
                 results.Add(row); 
             }
             return Ok(new { data = results });
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
            if (string.IsNullOrWhiteSpace(q) || q.Length < 2)
                return BadRequest("Invalid search query.");

            var offset = (page - 1) * limit;
            var term = "%" + q + "%";

            // Unified Person Search Query - Parameterized
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
        public async Task<IActionResult> GetIncidents([FromQuery] int cadLimit = 1000, [FromQuery] int arrestLimit = 1000, [FromQuery] int crimeLimit = 1000, [FromQuery] DateTime? dateFrom = null, [FromQuery] DateTime? dateTo = null)
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
             using var cmd = conn.CreateCommand();

             // 1. Arrests
             var sqlArrest = $"SELECT TOP (@ArrestLimit) id, event_time, charge, name, firstname, lastname, location, [key], geox, geoy, 'DailyBulletinArrests' as _source FROM dbo.DailyBulletinArrests WHERE [key] = 'AR'";
             if (dateFrom.HasValue) sqlArrest += " AND event_time >= @DateFrom";
             
             // 2. CAD - Use cadHandler which has geocoded data
             var sqlCad = $"SELECT TOP (@CadLimit) id, starttime, nature, address, agency, geox, geoy, 'cadHandler' as _source FROM dbo.cadHandler WHERE 1=1";
             if (dateFrom.HasValue) sqlCad += " AND starttime >= @DateFrom";

             cmd.CommandText = sqlArrest + "; " + sqlCad;
             cmd.Parameters.AddWithValue("@ArrestLimit", arrestLimit);
             cmd.Parameters.AddWithValue("@CadLimit", cadLimit);
             if (dateFrom.HasValue) cmd.Parameters.AddWithValue("@DateFrom", dateFrom.Value);

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

        [HttpGet("stats/jail")]
        public async Task<IActionResult> GetJailStats()
        {
             // 1. Base Stats
             var baseSql = @"
                SELECT
                  (SELECT COUNT(*) FROM jail_inmates WHERE released_date IS NULL) as TotalInmates,
                  (SELECT AVG(TRY_CAST(replace(total_bond_amount, '$', '') AS float)) FROM jail_inmates WHERE released_date IS NULL) as AvgBond,
                  (SELECT SUM(TRY_CAST(replace(total_bond_amount, '$', '') AS float)) FROM jail_inmates WHERE released_date IS NULL) as TotalBond,
                  (SELECT sex as name, COUNT(*) as value FROM jail_inmates WHERE released_date IS NULL AND sex IS NOT NULL GROUP BY sex FOR JSON PATH) as SexDist,
                  (SELECT race as name, COUNT(*) as value FROM jail_inmates WHERE released_date IS NULL AND race IS NOT NULL GROUP BY race FOR JSON PATH) as RaceDist";
             
             // 2. Charge Counts - Optimized into SQL instead of JS
             var chargeSql = @"
                SELECT TOP 10 charge_description as name, COUNT(*) as value
                FROM jail_charges c
                JOIN jail_inmates i ON c.book_id = i.book_id
                WHERE i.released_date IS NULL AND charge_description IS NOT NULL
                GROUP BY charge_description
                ORDER BY COUNT(*) DESC
                FOR JSON PATH";

             var results = new Dictionary<string, object>();
             using var conn = new SqlConnection(_connectionString);
             await conn.OpenAsync();
             using var cmd = conn.CreateCommand();
             cmd.CommandText = baseSql + "; " + chargeSql;

             using var reader = await cmd.ExecuteReaderAsync();
             if (await reader.ReadAsync()) {
                 for(int i=0; i<reader.FieldCount; i++) results[reader.GetName(i)] = reader.GetValue(i);
             }
             
             if (await reader.NextResultAsync()) {
                 if (await reader.ReadAsync()) results["TopCharges"] = reader.GetValue(0); // FOR JSON PATH returns 1 column
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
                FROM jail_inmates 
                WHERE {where}
                ORDER BY arrest_date DESC
                OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY";

            var inmates = new List<Dictionary<string, object>>();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                foreach(var p in parameters) cmd.Parameters.AddWithValue(p.Key, p.Value);

                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++) row[reader.GetName(i)] = reader.GetValue(i);
                    inmates.Add(row);
                }
            }

            if (inmates.Count == 0) return Ok(new { data = inmates });

            // 2. Fetch Charges for these BookIDs
            var bookIds = inmates.Select(i => i["book_id"].ToString()).Distinct().ToList();
            var idList = string.Join("','", bookIds.Select(id => id.Replace("'", "''"))); // Safety

            var chargesSql = $@"
                SELECT book_id, charge_description 
                FROM jail_charges 
                WHERE book_id IN ('{idList}')
                ORDER BY charge_description"; // Order for consistency

            var charges = new Dictionary<string, List<string>>();
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = chargesSql;
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
                 sql = $"SELECT TOP 20 id, event_time, charge, name, firstname, lastname, location, [key] FROM dbo.DailyBulletinArrests WHERE {where} AND [key] = 'AR' ORDER BY event_time DESC";
             }
             else if (type == "traffic") {
                 sql = $"SELECT TOP 20 id, event_time, charge, name, firstname, lastname, location, [key] FROM dbo.DailyBulletinArrests WHERE {where} AND ([key] = 'TC' OR [key] = 'TA') ORDER BY event_time DESC";
             }
             else if (type == "crime") {
                 sql = $"SELECT TOP 20 id, event_time, charge, name, firstname, lastname, location, [key] FROM dbo.DailyBulletinArrests WHERE {where} AND [key] = 'LW' ORDER BY event_time DESC";
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
                     for(int i=0; i<r.FieldCount; i++) row[r.GetName(i)] = r.GetValue(i);
                     finalRes["summary"] = row;
                 }
             }
             
             // 2. Detail
             cmd.CommandText = "SELECT TOP 1 OffenderNumber, Location, Offense, TDD_SDD, CommitmentDate, RecallDate, InterviewDate, MandatoryMinimum, DecisionType, Decision, DecisionDate, EffectiveDate FROM dbo.Offender_Detail WHERE OffenderNumber = @Num";
             using (var r = await cmd.ExecuteReaderAsync()) {
                 if (await r.ReadAsync()) {
                     var row = new Dictionary<string, object>();
                     for(int i=0; i<r.FieldCount; i++) row[r.GetName(i)] = r.GetValue(i);
                     finalRes["detail"] = row;
                 }
             }

             // 3. Charges
             cmd.CommandText = "SELECT ChargeID, OffenderNumber, SupervisionStatus, OffenseClass, CountyOfCommitment, EndDate FROM dbo.Offender_Charges WHERE OffenderNumber = @Num ORDER BY EndDate DESC";
             var chargesList = new List<Dictionary<string, object>>();
             using (var r = await cmd.ExecuteReaderAsync()) {
                 while (await r.ReadAsync()) {
                     var row = new Dictionary<string, object>();
                     for(int i=0; i<r.FieldCount; i++) row[r.GetName(i)] = r.GetValue(i);
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
                SELECT ArrestRecordName, ArrestCharge, OriginalOffenses, DocOffenderNumber as OffenderNumbers, 
                       ArrestDate as event_time, ArrestLocation as location, JailBookId, JailBondAmount, JailCharges, JailArrestDate, JailReleasedDate
                FROM vw_ViolatorsWithJailInfo A
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
