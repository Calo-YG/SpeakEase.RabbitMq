using Microsoft.Extensions.Logging;
using SpeakEase.RabbitMq.Attributes;
using SpeakEase.RabbitMq.Interface;

namespace SpeakEase.RabbitMq.Consumer
{
    [Inject(ServiceLifetime = ServiceLifetime.Transient)]
    internal class TestConsumer(ILogger<TestConsumer> logger): IConsumer<TestMessage>
    {
        public Task FailureAsync(TestMessage message, CancellationToken cancellationToken = default)
        {
            logger.LogInformation($"执行失败策略{message.MessaeId}-{message.Name}");

            return Task.CompletedTask;
        }

        public Task HandleAsync(TestMessage message, CancellationToken cancellationToken = default)
        {
            logger.LogInformation($"执行{message.MessaeId}-{message.Name}");

            return Task.CompletedTask;
        }
    }
}
