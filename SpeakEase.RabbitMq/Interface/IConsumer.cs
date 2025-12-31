namespace SpeakEase.RabbitMq.Interface;

/// <summary>
/// 消息消费者接口
/// </summary>
/// <typeparam name="TMessage">消息类型</typeparam>
public interface IConsumer<in TMessage>
{
    /// <summary>
    /// 处理消息
    /// </summary>
    /// <param name="message">消息内容</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    Task HandleAsync(TMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// 失败 回滚 补偿
    /// <param name="message"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task FailureAsync(TMessage message,CancellationToken cancellationToken = default);
}
