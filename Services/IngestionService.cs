using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using ScalableMssqlApi.Controllers;
using ScalableMssqlApi.Services.Interfaces;
using ScalableMssqlApi.DTOs;

namespace ScalableMssqlApi.Services
{
    public class IngestionService : IIngestionService
    {
        private readonly string _connectionString;
        private readonly ILogger<IngestionService> _logger;

        public IngestionService(IConfiguration config, ILogger<IngestionService> logger)
        {
            _connectionString = config["MssqlConnectionString"] 
                                ?? config["MSSQL_CONNECTION_STRING"]
                                ?? config.GetConnectionString("DefaultConnection")
                                ?? config.GetConnectionString("Default");
            _logger = logger;
        }

        public async Task<(int Inserted, int Skipped, List<string> InsertedIds)> IngestDailyBulletinAsync(List<DailyBulletinDto> reports)
        {
            if (reports == null || !reports.Any()) return (0, 0, new List<string>());

            var insertedIds = new List<string>();
            int skipped = 0;

            // Pre-process incoming data - collect both rowHashes and raw site_ids
            var incomingRowHashes = reports.Select(r => r.id).Distinct().ToList();
            var incomingSiteIds = reports.Select(r => r.site_id).Where(sid => !string.IsNullOrEmpty(sid)).Distinct().ToList();
            
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var tran = conn.BeginTransaction();

            try 
            {
                // Pull both exact hash matches AND records sharing the same raw site_id to execute deep logical comparisons
                var existingRecords = (await conn.QueryAsync(
                    "SELECT row_hash, site_id, name, time, [key] FROM DailyBulletinArrests WHERE row_hash IN @hashes OR site_id IN @siteIds", 
                    new { hashes = incomingRowHashes, siteIds = incomingSiteIds }, 
                    transaction: tran
                )).ToList();

                var existingHashes = existingRecords.Select(r => (string)r.row_hash).ToHashSet(StringComparer.OrdinalIgnoreCase);
                var toInsert = new List<DailyBulletinDto>();

                foreach (var r in reports)
                {
                    var incomingHash = r.id?.Trim().ToUpper() ?? "";

                    // Fast Path Check: Exact Hash Match
                    if (existingHashes.Contains(incomingHash))
                    {
                         skipped++;
                         continue;
                    } 
                    
                    // Deep Logical Check: Does this record exist under a slightly mutated hash?
                    var potentialLogicalDupes = existingRecords.Where(record => string.Equals((string)record.site_id, r.site_id, StringComparison.OrdinalIgnoreCase));
                    bool isLogicalDuplicate = false;
                    foreach (var potentialDupe in potentialLogicalDupes)
                    {
                        if (IsSame(potentialDupe, r))
                        {
                            isLogicalDuplicate = true;
                            break;
                        }
                    }

                    if (isLogicalDuplicate)
                    {
                        skipped++;
                        continue;
                    }

                    // Record is genuinely new
                    toInsert.Add(r);
                    insertedIds.Add(r.id);
                    existingHashes.Add(incomingHash); // prevent in-batch dupes
                }

                // 2. Bulk Insert using SqlBulkCopy
                if (toInsert.Any())
                {
                    var dataTable = new DataTable();
                    dataTable.Columns.Add("row_hash", typeof(string));
                    dataTable.Columns.Add("site_id", typeof(string));
                    dataTable.Columns.Add("invid", typeof(string));
                    dataTable.Columns.Add("key", typeof(string));
                    dataTable.Columns.Add("location", typeof(string));
                    dataTable.Columns.Add("name", typeof(string));
                    dataTable.Columns.Add("crime", typeof(string));
                    dataTable.Columns.Add("time", typeof(string));
                    dataTable.Columns.Add("property", typeof(string));
                    dataTable.Columns.Add("officer", typeof(string));
                    dataTable.Columns.Add("case", typeof(string));
                    dataTable.Columns.Add("description", typeof(string));
                    dataTable.Columns.Add("race", typeof(string));
                    dataTable.Columns.Add("sex", typeof(string));
                    dataTable.Columns.Add("lastname", typeof(string));
                    dataTable.Columns.Add("firstname", typeof(string));
                    dataTable.Columns.Add("charge", typeof(string));
                    dataTable.Columns.Add("middlename", typeof(string));
                    // Added columns based on SQL provided in previous steps / known schema
                    // If table has more columns like 'inserted_at', they might be defaults. 
                    // SqlBulkCopy only maps what we tell it.

                    foreach (var item in toInsert)
                    {
                        string safeHash = item.id?.Length > 50 ? item.id.Substring(0, 50) : item.id;
                        string safeSiteId = item.site_id?.Length > 50 ? item.site_id.Substring(0, 50) : item.site_id;
                        string safeKey = item.key?.Length > 50 ? item.key.Substring(0, 50) : item.key;
                        string safeLocation = item.location?.Length > 500 ? item.location.Substring(0, 500) : item.location;
                        string safeName = item.name?.Length > 255 ? item.name.Substring(0, 255) : item.name;
                        string safeCrime = item.crime?.Length > 500 ? item.crime.Substring(0, 500) : item.crime;
                        string safeTime = item.time?.Length > 100 ? item.time.Substring(0, 100) : item.time;
                        string safeProperty = item.property?.Length > 255 ? item.property.Substring(0, 255) : item.property;
                        string safeOfficer = item.officer?.Length > 255 ? item.officer.Substring(0, 255) : item.officer;
                        string safeCase = item.@case?.Length > 1500 ? item.@case.Substring(0, 1500) : item.@case;
                        string safeDescription = item.description?.Length > 1000 ? item.description.Substring(0, 1000) : item.description;
                        string safeRace = item.race?.Length > 100 ? item.race.Substring(0, 100) : item.race;
                        string safeSex = item.sex?.Length > 50 ? item.sex.Substring(0, 50) : item.sex;
                        string safeLastName = item.lastname?.Length > 100 ? item.lastname.Substring(0, 100) : item.lastname;
                        string safeFirstName = item.firstname?.Length > 100 ? item.firstname.Substring(0, 100) : item.firstname;
                        string safeCharge = item.charge?.Length > 500 ? item.charge.Substring(0, 500) : item.charge;
                        string safeMiddleName = item.middlename?.Length > 100 ? item.middlename.Substring(0, 100) : item.middlename;

                        dataTable.Rows.Add(
                            safeHash, safeSiteId, item.invid, safeKey, safeLocation, safeName, safeCrime, 
                            safeTime, safeProperty, safeOfficer, safeCase, safeDescription, 
                            safeRace, safeSex, safeLastName, safeFirstName, safeCharge, safeMiddleName
                        );
                    }

                    using var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tran);
                    bulkCopy.DestinationTableName = "DailyBulletinArrests";
                    
                    // Explicit Column Mapping is safer
                    foreach (DataColumn col in dataTable.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }

                    await bulkCopy.WriteToServerAsync(dataTable);
                }

                tran.Commit();
                return (toInsert.Count, skipped, insertedIds);

            }
            catch (Exception ex)
            {
                tran.Rollback();
                _logger.LogError(ex, "DailyBulletin Batch Failed");
                throw;
            }
        }

        private bool IsSame(dynamic dbRow, DailyBulletinDto incoming)
        {
            if (dbRow == null) return false;
            string dbName = (string)dbRow.name ?? "";
            string dbTime = (string)dbRow.time ?? "";
            string dbKey = (string)dbRow.key ?? "";
            string inName = incoming.name ?? "";
            string inTime = incoming.time ?? "";
            string inKey = incoming.key ?? "";

            return string.Equals(dbName.Trim(), inName.Trim(), StringComparison.OrdinalIgnoreCase) &&
                   string.Equals(dbTime.Trim(), inTime.Trim(), StringComparison.OrdinalIgnoreCase) &&
                   string.Equals(dbKey.Trim(), inKey.Trim(), StringComparison.OrdinalIgnoreCase);
        }
    }
}
