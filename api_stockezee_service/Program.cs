using api_stockezee_service.RedisService;
using api_stockezee_service.Service;
using Microsoft.AspNetCore.ResponseCompression;
using Npgsql;
using Scalar.AspNetCore;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

//// Add services to the container.
builder.Services.AddCors(options =>
{
    options.AddPolicy("CorsPolicy",
        builder => builder.AllowAnyOrigin()
        .AllowAnyMethod()
        .AllowAnyHeader());
});


// Add services to the container.

builder.Services.AddControllers().AddNewtonsoftJson();


//builder.Services.AddControllers(options =>
//{
//    // Add the custom validation filter globally
//    options.Filters.Add<CustomValidationFilter>();
//}).ConfigureApiBehaviorOptions(options =>
//{
//    options.SuppressModelStateInvalidFilter = true; // Disable default validation response
//}).AddNewtonsoftJson();

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

app.UseRouting();
app.UseCors("CorsPolicy");

app.UseAuthorization();

app.MapControllers();

app.MapOpenApi();
app.MapScalarApiReference(op =>
{
    op
    .WithDefaultHttpClient(ScalarTarget.CSharp, ScalarClient.RestSharp);
});

//app.MapScalarApiReference(opt =>
//{
//    opt.Title = "Scalar Example";
//    opt.Theme = ScalarTheme.Mars;
//    opt.DefaultHttpClient = new(ScalarTarget.CSharp, ScalarClient.RestSharp);
//    opt.BaseServerUrl = "https://api.stockezee.com";
//});


app.Run();

