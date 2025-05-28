using api_stockezee_service.RedisService;
using api_stockezee_service.Service;
using api_stockezee_service.Utility;
using Microsoft.AspNetCore.ResponseCompression;
using Npgsql;
using Scalar.AspNetCore;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddCors(options =>
{
    options.AddPolicy("CorsPolicy",
        builder => builder.AllowAnyOrigin()
        .AllowAnyMethod()
        .AllowAnyHeader());
});
//builder.Services.AddCors(options =>
//{
//    options.AddPolicy("CorsPolicy",
//         builder =>
//         {
//             builder
//             .SetIsOriginAllowedToAllowWildcardSubdomains()
//             .WithOrigins("http://stockezee.in", "https://stockezee.com", "http://*.stockezee.com", "https://*.stockezee.com", "http://localhost:3005") // Allow specific origins and subdomains
//             .AllowAnyHeader().AllowAnyMethod().AllowCredentials();
//         });
//});


// Add services to the container.

builder.Services.AddControllers(options =>
{
    // Add the custom validation filter globally
    options.Filters.Add<CustomValidationFilter>();
}).ConfigureApiBehaviorOptions(options =>
{
    options.SuppressModelStateInvalidFilter = true; // Disable default validation response
}).AddNewtonsoftJson();

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi

builder.Services.AddOpenApi();

builder.Services.Configure<GzipCompressionProviderOptions>(options =>
           options.Level = System.IO.Compression.CompressionLevel.Optimal);
builder.Services.AddResponseCompression(option =>
{
    option.EnableForHttps = true;
    option.Providers.Add<GzipCompressionProvider>();
}
);

// Register Redis connection as singleton
builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    // Get Redis connection string from configuration
    var redisConnectionString = builder.Configuration.GetConnectionString("Redis");
    return ConnectionMultiplexer.Connect(redisConnectionString);
});

builder.Services.AddSingleton<Func<NpgsqlConnection>>(sp =>
{
    return () => new NpgsqlConnection(builder.Configuration.GetConnectionString("PostgreSql"));
});

builder.Services.AddSingleton<PostgresBulkInsertService>();


// Register our Redis message handler service
builder.Services.AddHostedService<ResourceSubscriberService>();


builder.Services.AddSingleton<PgResourceDbService>();


var app = builder.Build();


app.UseResponseCompression();

app.UseHttpsRedirection();
//app.UseHsts();


app.UseRouting();
app.UseCors("CorsPolicy");

app.UseAuthorization();

app.MapControllers();

app.MapOpenApi();
app.MapScalarApiReference(op =>
{
    op
    .WithDefaultHttpClient(ScalarTarget.CSharp, ScalarClient.RestSharp);
    op.Theme = ScalarTheme.DeepSpace;
});


app.Run();

