// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SpeakEase.RabbitMq.Extensions;


Console.WriteLine("Start Consumer!");

var host = Host.CreateDefaultBuilder()
    .ConfigureHostConfiguration(cfg =>
    {
        cfg.AddJsonFile("Configurations/rabbitmq.json");
    })
    .ConfigureServices((cotext, cfg) =>
    {
        var configuration = cotext.Configuration;

        cfg.AddRabbitMq(configuration);
        cfg.AddInjectServices(); // 添加所有带 [Inject] 特性的服务
    })
    .ConfigureLogging(cfg =>
    {
        cfg.AddConsole();
    }).Build();


await host.Services.SubscribeAllConsumersAsync();

await host.RunAsync();