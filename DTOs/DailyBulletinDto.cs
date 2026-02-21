#nullable enable
namespace ScalableMssqlApi.DTOs
{
    public class DailyBulletinDto {
        // Raw Fields
        public string? invid { get; set; }
        public string? key { get; set; }
        public string? location { get; set; }
        public string? id { get; set; } // This now represents the computations row_hash
        public string? site_id { get; set; } // The new canonical remote ID
        public string? name { get; set; }
        public string? crime { get; set; }
        public string? time { get; set; }
        public string? property { get; set; }
        public string? officer { get; set; }
        public string? @case { get; set; } // 'case' is keyword
        public string? description { get; set; }
        public string? race { get; set; }
        public string? sex { get; set; }
        public string? lastname { get; set; }
        public string? firstname { get; set; }
        public string? charge { get; set; }
        public string? middlename { get; set; }
    }
}
