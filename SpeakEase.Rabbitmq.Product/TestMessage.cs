using SpeakEase.RabbitMq.Attributes;

namespace SpeakEase.Rabbitmq.Product
{
    [Consumer(ExchangeName = "SpeakEase.MQ",QueueName = "SpeakEase",RouteKey ="SpeakEase.Test")]
    public partial class TestMessage
    {
        public Guid MessaeId { get; set; }

        public string Name { get; set; }
    }
}
