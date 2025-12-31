namespace SpeakEase.RabbitMq.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public class ConsumerAttribute:Attribute
    {
        /// <summary>
        /// 交换机名称
        /// </summary>
        public string ExchangeName { get; set; }

        /// <summary>
        /// 队列名称
        /// </summary>
        public string QueueName { get; set; }

        /// <summary>
        /// 路由键名称
        /// </summary>
        public string RouteKey { get; set; }
    }
}
