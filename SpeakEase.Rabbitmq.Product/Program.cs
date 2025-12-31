// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SpeakEase.Rabbitmq.Product;
using SpeakEase.RabbitMq.Extensions;
using SpeakEase.RabbitMq.Interface;

Console.WriteLine("Hello, World!");

var host = Host.CreateDefaultBuilder()
    .ConfigureHostConfiguration(cfg =>
    {
        cfg.AddJsonFile("Configurations/rabbitmq.json");
    })
    .ConfigureServices((cotext,cfg) =>
    {
        var configuration = cotext.Configuration;

        cfg.AddRabbitMq(configuration);
    })
    .ConfigureLogging(cfg =>
    {
        cfg.AddConsole();
    }).Build();


var messagepushlish = host.Services.GetRequiredService<IMessagePublish>();

for (int i = 0; i < 10000; i++)
{
    var testmessage = new TestMessage
    {
        MessaeId = Guid.NewGuid(),
        Name = i.ToString(),
    };

    await  messagepushlish.PublishAsync(testmessage);
}

await host.RunAsync();