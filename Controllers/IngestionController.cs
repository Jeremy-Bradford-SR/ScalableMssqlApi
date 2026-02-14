using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Dapper;
using System.Data;
using System.Text.Json;
using System.Security.Cryptography;
using System.Text;
using ScalableMssqlApi.Services.Interfaces; // Use Service Interface
using ScalableMssqlApi.DTOs;

namespace ScalableMssqlApi.Controllers
{
    [ApiController]
    [Route("api/ingestion")]
    public class IngestionController : ControllerBase
    {
        private readonly string _connectionString; // Kept for legacy methods temporarily
        private readonly ILogger<IngestionController> _logger;
        private readonly IIngestionService _ingestionService;

        public IngestionController(IConfiguration config, ILogger<IngestionController> logger, IIngestionService ingestionService)
        {
            _connectionString = config["MssqlConnectionString"] ?? config.GetConnectionString("DefaultConnection");
            _logger = logger;
            _ingestionService = ingestionService;
        }

        // --- DTOs ---
        public class JailInmateDto {
            public string book_id { get; set; }
            public string invid { get; set; }
            public string firstname { get; set; }
            public string lastname { get; set; }
            public string middlename { get; set; }
            public string disp_name { get; set; }
            public int? age { get; set; }
            public DateTime? dob { get; set; }
            public string sex { get; set; }
            public string race { get; set; }
            public DateTime? arrest_date { get; set; }
            public string agency { get; set; }
            public string disp_agency { get; set; }
            public string total_bond_amount { get; set; }
            public DateTime? next_court_date { get; set; }
            public byte[] photo_data { get; set; } // Sent as base64 in JSON
            public List<JailChargeDto> charges { get; set; }
        }

        public class JailChargeDto {
            public string charge_description { get; set; }
            public string status { get; set; }
            public string docket_number { get; set; }
            public string bond_amount { get; set; }
            public string disp_charge { get; set; }
        }


        // ... (Usage of DTOs)
        public class JailBatchRequest {
            public List<JailInmateDto> Inmates { get; set; }
        }
        public class CadBatchRequest {
            public List<CadCallDto> Calls { get; set; }
        }
        public class SexOffenderBatchRequest {
            public List<SexOffenderDto> Registrants { get; set; }
        }

        [HttpPost("jail/sync")]
        public async Task<IActionResult> SyncJailInmates([FromBody] JailBatchRequest request)
        {
            if (request == null || request.Inmates == null || !request.Inmates.Any()) return BadRequest("No data provided");
            var inmates = request.Inmates;

            _logger.LogInformation($"Syncing {inmates.Count} inmates.");
            
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var tran = conn.BeginTransaction();

            try
            {
                // 1. Check existence for ALL incoming IDs to decide Insert vs Update
                // We cannot rely on 'released_date is null' because we might re-scrape a released inmate to update them (rare but possible)
                // or the 'active' list method was missing released inmates causing PK violation on Insert.
                var incomingIds = inmates.Select(x => x.book_id).ToHashSet();
                
                var existingIdsInDb = (await conn.QueryAsync<string>(
                    "SELECT book_id FROM jail_inmates WHERE book_id IN @ids", 
                    new { ids = incomingIds }, 
                    transaction: tran)).ToHashSet();

                int inserted = 0;
                int updated = 0;

                // 2. Upsert Logic
                var updateSql = @"
                    UPDATE jail_inmates SET
                        invid=@invid, firstname=@firstname, lastname=@lastname, middlename=@middlename, disp_name=@disp_name,
                        age=@age, dob=@dob, sex=@sex, race=@race, arrest_date=@arrest_date, agency=@agency, disp_agency=@disp_agency,
                        last_updated=GETDATE(), released_date=NULL, total_bond_amount=@total_bond_amount, next_court_date=@next_court_date
                    WHERE book_id=@book_id";

                var insertSql = @"
                    INSERT INTO jail_inmates (
                        book_id, invid, firstname, lastname, middlename, disp_name,
                        age, dob, sex, race, arrest_date, agency, disp_agency, total_bond_amount, next_court_date, last_updated
                    ) VALUES (
                        @book_id, @invid, @firstname, @lastname, @middlename, @disp_name,
                        @age, @dob, @sex, @race, @arrest_date, @agency, @disp_agency, @total_bond_amount, @next_court_date, GETDATE()
                    )";

                foreach (var inmate in inmates)
                {
                    bool exists = existingIdsInDb.Contains(inmate.book_id);

                    if (exists)
                    {
                        await conn.ExecuteAsync(updateSql, inmate, transaction: tran);
                        updated++;
                    }
                    else
                    {
                        await conn.ExecuteAsync(insertSql, inmate, transaction: tran);
                        inserted++;
                    }

                    // Charges (Delete/Insert)
                    await conn.ExecuteAsync("DELETE FROM jail_charges WHERE book_id=@book_id", new { book_id = inmate.book_id }, transaction: tran);
                    if (inmate.charges != null)
                    {
                        foreach(var ch in inmate.charges)
                        {
                            await conn.ExecuteAsync(@"
                                INSERT INTO jail_charges (book_id, charge_description, status, docket_number, bond_amount, disp_charge)
                                VALUES (@book_id, @charge_description, @status, @docket_number, @bond_amount, @disp_charge)",
                                new { 
                                    book_id = inmate.book_id, 
                                    ch.charge_description, ch.status, ch.docket_number, ch.bond_amount, ch.disp_charge 
                                }, transaction: tran);
                        }
                    }

                    // Photos (Upsert) - Only if provided
                    if (inmate.photo_data != null && inmate.photo_data.Length > 0)
                    {
                        var photoExists = await conn.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM jail_photos WHERE book_id=@book_id", new { book_id = inmate.book_id }, transaction: tran);
                        if (photoExists > 0)
                        {
                            await conn.ExecuteAsync("UPDATE jail_photos SET photo_data=@photo_data, last_updated=GETDATE() WHERE book_id=@book_id", new { book_id = inmate.book_id, inmate.photo_data }, transaction: tran);
                        }
                        else
                        {
                            await conn.ExecuteAsync("INSERT INTO jail_photos (book_id, photo_data, last_updated) VALUES (@book_id, @photo_data, GETDATE())", new { book_id = inmate.book_id, inmate.photo_data }, transaction: tran);
                        }
                    }
                }

                // 3. Release Logic
                // To detect releases, we need the list of CURRENTLY ACTIVE inmates in DB, and verify if they are missing from the incoming list.
                // BUT, since this endpoint might be called mainly with 'current roster', we should be careful.
                // If we are doing a FULL SYNC (all pages), then yes.
                // If we are doing a partial sync (one page), we cannot determine releases.
                // NOTE: The scraper usually fetches all pages.
                // However, without a 'sync_id' or 'batch_type', it's hard to know if this is a partial or full list.
                // Original logic: active_ids in DB that are NOT in incoming list -> Set Released.
                // This logic is dangerous if scraping is chunked/paginated.
                // Assuming the scraper sends the FULL list in one go? 
                // No, scraper sends batches! Scraper code loop: `process_batch(executor, inmates)` (10-20 at a time).
                // DO NOT RELEASE based on a single batch! We will inadvertently release everyone else!
                
                // FIX: Remove Release Logic from Batch Sync. Release detection should be a separate process or handled differently.
                // For now, I will comment out the release logic to prevent data loss (false releases).
                /*
                if (inmates.Count > 0) 
                {
                    var activeIds = (await conn.QueryAsync<string>("SELECT book_id FROM jail_inmates WHERE released_date IS NULL", transaction: tran)).ToHashSet();
                    var releasedParams = activeIds.Where(id => !incomingIds.Contains(id)).Select(id => new { book_id = id }).ToList();
                    if (releasedParams.Any())
                    {
                        // DANGEROUS IN BATCH MODE
                        // await conn.ExecuteAsync("UPDATE jail_inmates SET released_date = GETDATE() WHERE book_id = @book_id", releasedParams, transaction: tran);
                    }
                }
                */

                tran.Commit();
                return Ok(new { inserted, updated });

            }
            catch (Exception ex)
            {
                tran.Rollback();
                _logger.LogError(ex, "Sync failed");
                return StatusCode(500, ex.Message);
            }
        }
        // --- Recent Calls ---
        public class CadCallDto {
            public string id { get; set; }
            public string invid { get; set; }
            public DateTime? starttime { get; set; }
            public DateTime? closetime { get; set; }
            public string agency { get; set; }
            public string service { get; set; }
            public string nature { get; set; }
            public string address { get; set; }
            public float? geox { get; set; }
            public float? geoy { get; set; }
            public string marker_details_xml { get; set; }
            public string rec_key { get; set; }
            public string icon_url { get; set; }
            public string icon { get; set; }
        }

        [HttpPost("recent-calls/batch")]
        public async Task<IActionResult> BatchRecentCalls([FromBody] CadBatchRequest request)
        {
             if (request == null || request.Calls == null || !request.Calls.Any()) return Ok(new { inserted = 0, skipped = 0 });
             var calls = request.Calls;
             
             using var conn = new SqlConnection(_connectionString);
             await conn.OpenAsync();
             using var tran = conn.BeginTransaction();
             
             int inserted = 0;
             int skipped = 0;
             var newIds = new List<string>();

             try {
                 foreach(var call in calls) {
                     var exists = await conn.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM CadHandler WHERE id=@id", new { id = call.id }, transaction: tran);
                     if (exists > 0) {
                         skipped++;
                     } else {
                         await conn.ExecuteAsync(@"
                             INSERT INTO CadHandler (invid, starttime, closetime, id, agency, service, nature, address, geox, geoy, geog, marker_details_xml, rec_key, icon_url, icon)
                             VALUES (@invid, @starttime, @closetime, @id, @agency, @service, @nature, @address, @geox, @geoy, NULL, @marker_details_xml, @rec_key, @icon_url, @icon)",
                             call, transaction: tran);
                         inserted++;
                         newIds.Add(call.id);
                     }
                 }
                 tran.Commit();
                 return Ok(new { inserted, skipped, insertedIds = newIds });
             } catch (Exception ex) {
                 tran.Rollback();
                 _logger.LogError(ex, "Recent Calls Batch Failed");
                 return StatusCode(500, ex.Message);
             }
        }

        // --- Sex Offenders ---
        public class SexOffenderDto {
             public string registrant_id { get; set; }
             public string oci { get; set; }
             public string last_name { get; set; }
             public string first_name { get; set; }
             public string middle_name { get; set; }
             public string gender { get; set; }
             public string tier { get; set; }
             public string race { get; set; }
             public string hair_color { get; set; }
             public string eye_color { get; set; }
             public string height_inches { get; set; }
             public string weight_pounds { get; set; }
             public string address_line_1 { get; set; }
             public string address_line_2 { get; set; }
             public string city { get; set; }
             public string state { get; set; }
             public string postal_code { get; set; }
             public string county { get; set; }
             public string lat { get; set; }
             public string lon { get; set; }
             public DateTime? birthdate { get; set; }
             public int? victim_minors { get; set; }
             public int? victim_adults { get; set; }
             public int? victim_unknown { get; set; }
             public string registrant_cluster { get; set; }
             public string photo_url { get; set; }
             public double? distance { get; set; }
             public DateTime? last_changed { get; set; }
             public byte[] photo_data { get; set; }
             public List<SoConvictionDto> conviction_list { get; set; }
             public List<SoAliasDto> alias_list { get; set; } 
             public List<string> markings { get; set; }
        }
        
        public class SoConvictionDto {
             public string conviction_text { get; set; }
             public string registrant_age { get; set; }
             public List<SoVictimDto> victims { get; set; }
        }
        public class SoVictimDto {
             public string gender { get; set; }
             public string age_group { get; set; }
        }
        public class SoAliasDto {
             public string last_name { get; set; }
             public string first_name { get; set; }
             public string middle_name { get; set; }
        }

        [HttpPost("sex-offenders/batch")]
        public async Task<IActionResult> BatchSexOffenders([FromBody] SexOffenderBatchRequest request) {
             if (request == null || request.Registrants == null || !request.Registrants.Any()) return Ok();
             var registrants = request.Registrants;

             using var conn = new SqlConnection(_connectionString);
             await conn.OpenAsync();
             using var tran = conn.BeginTransaction();
             
             try {
                foreach(var reg in registrants) {
                   var exists = await conn.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM sexoffender_registrants WHERE registrant_id=@registrant_id", new { reg.registrant_id }, transaction: tran);
                   
                   var upsertSql = exists > 0 ? 
                       @"UPDATE sexoffender_registrants SET 
                           oci=@oci, last_name=@last_name, first_name=@first_name, middle_name=@middle_name, gender=@gender, 
                           tier=@tier, race=@race, hair_color=@hair_color, eye_color=@eye_color, height_inches=@height_inches, 
                           weight_pounds=@weight_pounds, address_line_1=@address_line_1, address_line_2=@address_line_2, 
                           city=@city, state=@state, postal_code=@postal_code, county=@county, lat=@lat, lon=@lon, 
                           birthdate=@birthdate, victim_minors=@victim_minors, victim_adults=@victim_adults, 
                           victim_unknown=@victim_unknown, registrant_cluster=@registrant_cluster, photo_url=@photo_url, 
                           distance=@distance, last_changed=@last_changed, updated_at=GETDATE()
                         WHERE registrant_id=@registrant_id" :
                       @"INSERT INTO sexoffender_registrants (
                           registrant_id, oci, last_name, first_name, middle_name, gender, tier, race, hair_color, eye_color,
                           height_inches, weight_pounds, address_line_1, address_line_2, city, state, postal_code, county,
                           lat, lon, birthdate, victim_minors, victim_adults, victim_unknown, registrant_cluster, photo_url,
                           distance, last_changed
                         ) VALUES (
                           @registrant_id, @oci, @last_name, @first_name, @middle_name, @gender, @tier, @race, @hair_color, @eye_color,
                           @height_inches, @weight_pounds, @address_line_1, @address_line_2, @city, @state, @postal_code, @county,
                           @lat, @lon, @birthdate, @victim_minors, @victim_adults, @victim_unknown, @registrant_cluster, @photo_url,
                           @distance, @last_changed
                         )";
                         
                   await conn.ExecuteAsync(upsertSql, reg, transaction: tran);
                   
                   if (reg.photo_data != null) {
                       await conn.ExecuteAsync("UPDATE sexoffender_registrants SET photo_data=@photo_data WHERE registrant_id=@registrant_id", new { reg.photo_data, reg.registrant_id }, transaction: tran);
                   }

                   // Children
                   await conn.ExecuteAsync("DELETE FROM sexoffender_convictions WHERE registrant_id=@registrant_id", new { reg.registrant_id }, transaction: tran);
                   await conn.ExecuteAsync("DELETE FROM sexoffender_aliases WHERE registrant_id=@registrant_id", new { reg.registrant_id }, transaction: tran);
                   await conn.ExecuteAsync("DELETE FROM sexoffender_skin_markings WHERE registrant_id=@registrant_id", new { reg.registrant_id }, transaction: tran);

                   if (reg.conviction_list != null) {
                       foreach(var c in reg.conviction_list) {
                           // Insert Conviction and get ID
                           var apiId = await conn.QuerySingleAsync<int>(@"
                               INSERT INTO sexoffender_convictions (registrant_id, conviction_text, registrant_age) 
                               OUTPUT INSERTED.conviction_id 
                               VALUES (@registrant_id, @conviction_text, @registrant_age)", 
                               new { reg.registrant_id, c.conviction_text, c.registrant_age }, transaction: tran);
                           
                           if (c.victims != null) {
                               foreach(var v in c.victims) {
                                  await conn.ExecuteAsync("INSERT INTO sexoffender_conviction_victims (conviction_id, gender, age_group) VALUES (@apiId, @gender, @age_group)",
                                      new { apiId, v.gender, v.age_group }, transaction: tran);
                               }
                           }
                       }
                   }
                   if (reg.alias_list != null) {
                       foreach(var a in reg.alias_list) {
                           await conn.ExecuteAsync("INSERT INTO sexoffender_aliases (registrant_id, last_name, first_name, middle_name) VALUES (@registrant_id, @last_name, @first_name, @middle_name)",
                               new { reg.registrant_id, a.last_name, a.first_name, a.middle_name }, transaction: tran);
                       }
                   }
                   if (reg.markings != null) {
                       foreach(var m in reg.markings) {
                           await conn.ExecuteAsync("INSERT INTO sexoffender_skin_markings (registrant_id, marking_value) VALUES (@registrant_id, @marking_value)",
                               new { reg.registrant_id, marking_value = m }, transaction: tran);
                       }
                   }
                }
                tran.Commit();
                return Ok(new { count = registrants.Count });
             } catch (Exception ex) {
                 tran.Rollback();
                 _logger.LogError(ex, "SexOffender Batch Failed");
                 return StatusCode(500, ex.Message);
             }
        }

        // --- DOC ---
        public class DocSummaryDto {
            public string OffenderNumber { get; set; }
            public string Name { get; set; }
            public string Gender { get; set; }
            public string Age { get; set; }
        }
        public class DocDetailDto {
            public string OffenderNumber { get; set; }
            public string? Location { get; set; }
            public string Offense { get; set; }
            public DateTime? TDD_SDD { get; set; }
            public DateTime? CommitmentDate { get; set; }
            public DateTime? RecallDate { get; set; }
            public string InterviewDate { get; set; }
            public string MandatoryMinimum { get; set; }
            public string DecisionType { get; set; }
            public string? Decision { get; set; }
            public DateTime? DecisionDate { get; set; }
            public DateTime? EffectiveDate { get; set; }
            public List<DocChargeDto> Charges { get; set; }
        }
        public class DocChargeDto {
            public string SupervisionStatus { get; set; }
            public string OffenseClass { get; set; }
            public string? CountyOfCommitment { get; set; }
            public DateTime? EndDate { get; set; }
        }

        [HttpPost("doc/batch-summary")]
        public async Task<IActionResult> BatchDocSummary([FromBody] List<DocSummaryDto> summaries) {
            if (!summaries.Any()) return Ok();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var tran = conn.BeginTransaction();
            try {
                // Bulk Insert using basic Loop (Dapper Executemany equivalent)
                await conn.ExecuteAsync(@"
                    IF NOT EXISTS (SELECT 1 FROM Offender_Summary WHERE OffenderNumber = @OffenderNumber)
                    BEGIN
                        INSERT INTO Offender_Summary (OffenderNumber, Name, Gender, Age) VALUES (@OffenderNumber, @Name, @Gender, @Age)
                    END", summaries, transaction: tran);
                tran.Commit();
                return Ok();
            } catch (Exception ex) {
                tran.Rollback();
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPost("doc/batch-details")]
        public async Task<IActionResult> BatchDocDetails([FromBody] List<DocDetailDto> details) {
            if (!details.Any()) return Ok();
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var tran = conn.BeginTransaction();
            try {
                foreach(var d in details) {
                   await conn.ExecuteAsync(@"
                       IF EXISTS (SELECT 1 FROM Offender_Detail WHERE OffenderNumber = @OffenderNumber)
                           UPDATE Offender_Detail SET Location=@Location, Offense=@Offense, TDD_SDD=@TDD_SDD, CommitmentDate=@CommitmentDate, RecallDate=@RecallDate, InterviewDate=@InterviewDate, MandatoryMinimum=@MandatoryMinimum, DecisionType=@DecisionType, Decision=@Decision, DecisionDate=@DecisionDate, EffectiveDate=@EffectiveDate WHERE OffenderNumber=@OffenderNumber
                       ELSE
                           INSERT INTO Offender_Detail (OffenderNumber, Location, Offense, TDD_SDD, CommitmentDate, RecallDate, InterviewDate, MandatoryMinimum, DecisionType, Decision, DecisionDate, EffectiveDate) VALUES (@OffenderNumber, @Location, @Offense, @TDD_SDD, @CommitmentDate, @RecallDate, @InterviewDate, @MandatoryMinimum, @DecisionType, @Decision, @DecisionDate, @EffectiveDate)",
                       d, transaction: tran);

                   await conn.ExecuteAsync("DELETE FROM Offender_Charges WHERE OffenderNumber=@OffenderNumber", new { d.OffenderNumber }, transaction: tran);
                   if (d.Charges != null) {
                       foreach(var c in d.Charges) {
                           await conn.ExecuteAsync("INSERT INTO Offender_Charges (OffenderNumber, SupervisionStatus, OffenseClass, CountyOfCommitment, EndDate) VALUES (@OffenderNumber, @SupervisionStatus, @OffenseClass, @CountyOfCommitment, @EndDate)",
                               new { d.OffenderNumber, c.SupervisionStatus, c.OffenseClass, c.CountyOfCommitment, c.EndDate }, transaction: tran);
                       }
                   }
                }
                tran.Commit();
                return Ok();
            } catch (Exception ex) {
                tran.Rollback();
                return StatusCode(500, ex.Message);
            }
        }
        // --- Daily Bulletin ---
// DailyBulletinDto moved to ScalableMssqlApi.DTOs

        [HttpPost("daily-bulletin/batch")]
        public async Task<IActionResult> BatchDailyBulletin([FromBody] List<DailyBulletinDto> reports) {
            try {
                var result = await _ingestionService.IngestDailyBulletinAsync(reports);
                return Ok(new { inserted = result.Inserted, skipped = result.Skipped, insertedIds = result.InsertedIds });
            } catch (Exception ex) {
                _logger.LogError(ex, "DailyBulletin Batch Failed");
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("daily-bulletin/stats")]
        public async Task<IActionResult> GetDailyBulletinStats([FromQuery] string date) {
            // Expects date in YYYY-MM-DD
            if (!DateTime.TryParse(date, out var dt)) return BadRequest("Invalid date format");
            
            // Format to M/d/yyyy to match [time] string prefix in DB (e.g. 2/7/2026)
            // Note: DB string might not have leading zeros. 
            // Better strategy: Filter by parsed date if possible, but [time] is varchar.
            // Let's try matching the M/d/yyyy prefix.
            string prefix = $"{dt.Month}/{dt.Day}/{dt.Year}";
            
            using var conn = new SqlConnection(_connectionString);
            // Count by Key (AR, TC, etc)
            var sql = @"
                SELECT [key] as Type, COUNT(*) as Count 
                FROM DailyBulletinArrests 
                WHERE [time] LIKE @prefix + '%'
                GROUP BY [key]
            ";
            
            var stats = await conn.QueryAsync(sql, new { prefix });
            return Ok(stats);
        }
    }
}
