using SpeakEase.RabbitMq.Attributes;
using System.Text.Json.Serialization;

namespace SpeakEase.RabbitMq.Consumer
{
    [Consumer(ExchangeName = "SpeakEase.MQ",QueueName = "SpeakEase",RouteKey ="SpeakEase.Test")]
    public partial class TestMessage
    {
        public Guid MessaeId { get; set; }

        public string Name { get; set; }
    }
}
