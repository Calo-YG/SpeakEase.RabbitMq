namespace SpeakEase.RabbitMq.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public class InjectAttribute: Attribute
    {
        public ServiceLifetime ServiceLifetime { get; set; }
    }
}
