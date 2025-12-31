namespace SpeakEase.RabbitMq.Interface
{
    /// <summary>
    /// 消费者工厂接口
    /// </summary>
    public interface IConsumerFactory
    {
        /// <summary>
        /// 订阅消息
        /// </summary>
        /// <typeparam name="TMessage">消息类型</typeparam>
        Task SubscribeAsync<TMessage>();

        /// <summary>
        /// 取消订阅消息
        /// </summary>
        /// <typeparam name="TMessage">消息类型</typeparam>
        Task UnsubscribeAsync<TMessage>();
    }
}
