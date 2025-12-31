namespace SpeakEase.RabbitMq.Serialization
{
    /// <summary>
    /// 消息序列化器接口
    /// </summary>
    public interface IMessageSerializer
    {
        /// <summary>
        /// 序列化消息
        /// </summary>
        byte[] Serialize<TMessage>(TMessage message);

        /// <summary>
        /// 反序列化消息
        /// </summary>
        TMessage Deserialize<TMessage>(byte[] data);

        /// <summary>
        /// 获取内容类型
        /// </summary>
        string ContentType { get; }
    }
}
