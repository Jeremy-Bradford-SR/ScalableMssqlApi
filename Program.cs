using System.Reflection;

var builder = WebApplication.CreateBuilder(args);

// Logging
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

// Services
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
    var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
    c.IncludeXmlComments(xmlPath);
});
builder.Services.AddScoped<ScalableMssqlApi.Services.Interfaces.IIngestionService, ScalableMssqlApi.Services.IngestionService>();
builder.Services.AddScoped<ScalableMssqlApi.Services.Interfaces.IDataService, ScalableMssqlApi.Services.DataService>();

// Add CORS
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowAll",
        builder =>
        {
            builder.AllowAnyOrigin()
                   .AllowAnyMethod()
                   .AllowAnyHeader();
        });
});

// Add Health Checks
builder.Services.AddHealthChecks();

var app = builder.Build();

// Middleware
app.UseSwagger();
app.UseSwaggerUI(c =>
{
    c.SwaggerEndpoint("/swagger/v1/swagger.json", "Scalable MSSQL API v1");
    c.RoutePrefix = string.Empty; // Serve Swagger UI at root
});

app.UseCors("AllowAll");

// Register API Key Middleware
app.UseMiddleware<ScalableMssqlApi.Middleware.ApiKeyMiddleware>();


app.MapControllers();
app.MapHealthChecks("/health");

try
{
    app.Run();
}
catch (Exception ex)
{
    Console.WriteLine($"Startup failed: {ex}");
}
