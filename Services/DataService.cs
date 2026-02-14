using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using ScalableMssqlApi.Services.Interfaces;

namespace ScalableMssqlApi.Services
{
    public class DataService : IDataService
    {
        private readonly string _connectionString;

        public DataService(IConfiguration config)
        {
            _connectionString = config["MssqlConnectionString"] 
                                ?? config.GetConnectionString("DefaultConnection") 
                                ?? config.GetConnectionString("Default");
                                
            if (string.IsNullOrEmpty(_connectionString))
            {
                throw new InvalidOperationException("Missing connection string. Checked 'MssqlConnectionString', 'DefaultConnection', and 'Default'.");
            }
        }

        public async Task<List<Dictionary<string, object>>> QueryAsync(string table, string? columns, string? filters, string? orderBy, int? limit, int? offset)
        {
            // ... (rest of QueryAsync start) ...
            if (string.IsNullOrWhiteSpace(table)) throw new ArgumentException("Table name is required.");

            var safeTable = SanitizeAndQuote(table);
            string safeColumns = "*";

            if (!string.IsNullOrWhiteSpace(columns))
            {
                var colList = columns.Split(',', StringSplitOptions.RemoveEmptyEntries)
                                     .Select(c => SanitizeAndQuote(c.Trim()));
                safeColumns = string.Join(", ", colList);
            }

            var p = new DynamicParameters();
            var whereBuilder = new StringBuilder("1=1");

            if (!string.IsNullOrWhiteSpace(filters))
            {
                var (whereClause, paramsList) = ParseFilters(filters, p);
                whereBuilder.Append(" AND " + whereClause);
            }

            // Default Order
            if (string.IsNullOrWhiteSpace(orderBy)) orderBy = "id DESC"; // Default
            else orderBy = SanitizeOrderBy(orderBy);

            // Pagination
            string pagination = "";
            if (limit.HasValue)
            {
                int off = offset ?? 0;
                p.Add("@Limit", limit.Value);
                p.Add("@Offset", off);
                pagination = "OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY";
            }

            var sql = $"SELECT {safeColumns} FROM {safeTable} WHERE {whereBuilder} ORDER BY {orderBy} {pagination}";

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            
             // Ensure Dapper uses the parameters
            var result = await conn.QueryAsync<dynamic>(sql, p);
            
            // Materialize and cast to Dictionary for JSON serialization
            return result.Select(r => (IDictionary<string, object>)r).Select(d => new Dictionary<string, object>(d)).ToList();
        }

        // --- Helpers ---

        private (string WhereClause, DynamicParameters Params) ParseFilters(string filters, DynamicParameters p)
        {
             var sb = new StringBuilder();
            var conditions = System.Text.RegularExpressions.Regex.Split(filters, @"\s+AND\s+", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            
            int paramIndex = 0;
            
            foreach (var cond in conditions)
            {
                var match = System.Text.RegularExpressions.Regex.Match(cond.Trim(), @"^([a-zA-Z0-9_\.]+)\s*(=|!=|<>|>|<|>=|<=|LIKE)\s*(.+)$", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                if (!match.Success) throw new ArgumentException($"Invalid condition syntax: {cond}");
                
                var col = match.Groups[1].Value;
                var op = match.Groups[2].Value.ToUpper();
                var valRaw = match.Groups[3].Value.Trim();
                
                var safeCol = SanitizeAndQuote(col);
                
                var allowedOps = new HashSet<string> { "=", "!=", "<>", ">", "<", ">=", "<=", "LIKE" };
                if (!allowedOps.Contains(op)) throw new ArgumentException($"Invalid operator: {op}");
                
                object valParam;
                if (valRaw.StartsWith("'") && valRaw.EndsWith("'")) valParam = valRaw.Substring(1, valRaw.Length - 2);
                else if (double.TryParse(valRaw, out double dVal)) valParam = dVal;
                else if (valRaw.Equals("NULL", StringComparison.OrdinalIgnoreCase)) {
                    valParam = null;
                   if (op == "=") op = "IS"; else if (op == "!=" || op == "<>") op = "IS NOT";
                }
                else throw new ArgumentException($"Invalid value format: {valRaw}");

                if (sb.Length > 0) sb.Append(" AND ");
                
                if (valParam == null) sb.Append($"{safeCol} {op} NULL");
                else {
                     string pName = $"@p{paramIndex++}";
                     p.Add(pName, valParam);
                     sb.Append($"{safeCol} {op} {pName}");
                }
            }
            return (sb.ToString(), p);
        }

        private static string SanitizeAndQuote(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier) || identifier.Contains('\'') || identifier.Contains(';'))
                throw new ArgumentException("Invalid identifier.");
            return string.Join(".", identifier.Split('.').Select(part => $"[{part.Replace("]", "")}]"));
        }

        private static string SanitizeOrderBy(string orderBy)
        {
            if (System.Text.RegularExpressions.Regex.IsMatch(orderBy, @"[^a-zA-Z0-9_\.\s,DESCASC]", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                throw new ArgumentException("Invalid orderBy clause.");
            return orderBy;
        }
    }
}
