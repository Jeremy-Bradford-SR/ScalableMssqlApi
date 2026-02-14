using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;

namespace ScalableMssqlApi.Middleware
{
    public class ApiKeyMiddleware
    {
        private readonly RequestDelegate _next;
        private const string APIKEYNAME = "X-API-KEY";
        private readonly string _apiKey;

        public ApiKeyMiddleware(RequestDelegate next, IConfiguration configuration)
        {
            _next = next;
            // Prefer Env Var, fallback to config
            _apiKey = System.Environment.GetEnvironmentVariable("API_KEY") ?? configuration["ApiKey"];
        }

        public async Task InvokeAsync(HttpContext context)
        {
            // Protect all API endpoints. 
            // The Proxy (trusted) injects the Key. 
            // Internal Scripts must also provide the Key.
            if (context.Request.Path.StartsWithSegments("/api")) 
            {
                if (!context.Request.Headers.TryGetValue(APIKEYNAME, out var extractedApiKey))
                {
                    context.Response.StatusCode = 401;
                    await context.Response.WriteAsync("API Key was not provided.");
                    return;
                }

                if (string.IsNullOrEmpty(_apiKey) || !_apiKey.Equals(extractedApiKey))
                {
                    context.Response.StatusCode = 403;
                    await context.Response.WriteAsync("Unauthorized client.");
                    return;
                }
            }

            await _next(context);
        }
    }
}
