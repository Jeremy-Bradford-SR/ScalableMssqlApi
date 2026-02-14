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

            // 1. Pre-process incoming data
            // Strategy: Fetch existing records based on ID + Key combination to detect exact duplicates.
            // We do NOT rename IDs anymore. If (ID, Key) exists, we check if it's the same record.
            
            var compositeKeys = reports.Select(r => new { r.id, r.key }).Distinct().ToList();
            
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var tran = conn.BeginTransaction();

            try 
            {
                // Fetch existing records for collision detection
                // We fetch specific columns to compare content. 
                // Since we can't easily pass a list of composite keys to SQL without a TVP or complex query construction,
                // and fetching ALL by ID might be too much if IDs are reused broadly (though likely not THAT broadly),
                // we'll fetch by ID IN (...) and then filter in memory by Key.
                // This is a reasonable compromise if the number of distinct IDs in a batch isn't massive.
                
                var distinctIds = compositeKeys.Select(k => k.id).Distinct().ToList();

                var existingRecords = (await conn.QueryAsync<dynamic>(
                    "SELECT id, [key], name, [time] FROM DailyBulletinArrests WHERE id IN @ids", 
                    new { ids = distinctIds }, 
                    transaction: tran
                )).ToList(); // List of dynamic objects

                // Create a lookup for (id, key) -> list of records (technically should be 1 if constraint exists, but let's be safe)
                var existingLookup = existingRecords
                    .GroupBy(x => new { Id = (string)x.id, Key = (string)x.key })
                    .ToDictionary(g => g.Key, g => g.First());

                var toInsert = new List<DailyBulletinDto>();

                foreach (var r in reports)
                {
                    bool isDuplicate = false;
                    var lookupKey = new { Id = r.id, Key = r.key };

                    if (existingLookup.TryGetValue(lookupKey, out var existing))
                    {
                         // Record with same ID and Key exists. 
                         // Check if content matches (True Duplicate) or if it's a constraint violation we should skip?
                         // If it exists, we generally assume it's already ingested. 
                         // We can check IsSame to be sure, but if the ID+Key matches, we can't insert it anyway due to the UNIQUE constraint.
                         // So effectively, if ID+Key exists, we skip.
                         isDuplicate = true;
                         
                         // Optional: Log if content is different? 
                         // For now, simple skip logic as per "detect duplicates".
                    }

                    if (isDuplicate) {
                        skipped++;
                    } else {
                        toInsert.Add(r);
                        insertedIds.Add(r.id);
                        // Add to lookup to prevent self-collisions within the batch
                        try {
                            // We need to match the dynamic structure expected by IsSame: name, time, key (strings)
                            // We use an anonymous type which Dapper dynamic interaction should handle if we cast or just use object.
                            // Accessing it via `dynamic` later in TryGetValue -> out existing -> IsSame(existing...) works if properties exist.
                            
                            dynamic newRecord = new System.Dynamic.ExpandoObject();
                            ((IDictionary<string, object>)newRecord)["name"] = r.name;
                            ((IDictionary<string, object>)newRecord)["time"] = r.time;
                            ((IDictionary<string, object>)newRecord)["key"] = r.key;
                            ((IDictionary<string, object>)newRecord)["id"] = r.id;

                            if (!existingLookup.ContainsKey(lookupKey)) {
                                existingLookup.Add(lookupKey, newRecord);
                            }
                        } catch (Exception ex) {
                            _logger.LogWarning(ex, "Failed to update lookup for in-batch duplication check");
                        }
                    }
                }

                // 2. Bulk Insert using SqlBulkCopy
                if (toInsert.Any())
                {
                    var dataTable = new DataTable();
                    dataTable.Columns.Add("invid", typeof(string));
                    dataTable.Columns.Add("key", typeof(string));
                    dataTable.Columns.Add("location", typeof(string));
                    dataTable.Columns.Add("id", typeof(string));
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
                        dataTable.Rows.Add(
                            item.invid, item.key, item.location, item.id, item.name, item.crime, 
                            item.time, item.property, item.officer, item.@case, item.description, 
                            item.race, item.sex, item.lastname, item.firstname, item.charge, item.middlename
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
