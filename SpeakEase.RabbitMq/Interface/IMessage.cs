namespace SpeakEase.RabbitMq.Interface
{
    public interface IMessage
    {
        /// <summary>
        /// 交换机名称
        /// </summary>
        public string ExchangeName { get; }

        /// <summary>
        /// 队列名称
        /// </summary>
        public string QueeuName { get; }

        /// <summary>
        /// 路由名称
        /// </summary>
        public string RouteKey { get; }

        /// <summary>
        /// 消息类型
        /// </summary>
        public string Type {get;set;}
    }
}
