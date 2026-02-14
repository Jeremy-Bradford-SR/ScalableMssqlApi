using ScalableMssqlApi.DTOs;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ScalableMssqlApi.Services.Interfaces
{
    public interface IIngestionService
    {
        Task<(int Inserted, int Skipped, List<string> InsertedIds)> IngestDailyBulletinAsync(List<DailyBulletinDto> reports);
        // Add other methods here as we migrate them (JailInmates, etc.)
    }
}
