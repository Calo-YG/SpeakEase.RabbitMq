using SpeakEase.RabbitMq.Attributes;

namespace SpeakEase.RabbitMq.Consumer
{
    [Consumer(ExchangeName = "SpeakEase.MQ",QueueName = "SpeakEase",RouteKey ="SpeakEase.Test")]
    public class TestMessage
    {
        public Guid MessaeId { get; set; }

        public string Name { get; set; }
    }
}
