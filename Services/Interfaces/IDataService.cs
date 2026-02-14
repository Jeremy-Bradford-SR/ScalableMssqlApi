using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;

namespace ScalableMssqlApi.Services.Interfaces
{
    public interface IDataService
    {
        Task<List<Dictionary<string, object>>> QueryAsync(string table, string? columns, string? filters, string? orderBy, int? limit, int? offset);
        // Add other read methods here later
    }
}
