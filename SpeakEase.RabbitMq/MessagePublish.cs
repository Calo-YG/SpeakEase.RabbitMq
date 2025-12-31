using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using SpeakEase.RabbitMq.Cache;
using SpeakEase.RabbitMq.Connection;
using SpeakEase.RabbitMq.Interface;
using SpeakEase.RabbitMq.Serialization;

namespace SpeakEase.RabbitMq
{
    /// <summary>
    /// 消息发布器实现
    /// </summary>
    public class MessagePublish(
        IRabbitMqConnectionFactory connectionFactory,
        ILogger<MessagePublish> logger,
        IConsumerRouteCache routeCache,
        IMessageSerializer serializer) : IMessagePublish
    {

        /// <summary>
        /// 发布消息
        /// </summary>
        public async Task PublishAsync<Temessage>(Temessage message)
        {
            await PublishInternalAsync(message, 0, false);
        }

        /// <summary>
        /// 发布延时消息（使用 x-delayed-message 插件）
        /// </summary>
        public async Task PublishDelayAsync<Temessage>(Temessage message)
        {
            // 默认延迟 5 秒，你可以根据需要调整或添加参数
            await PublishInternalAsync(message, 5000, true);
        }

        /// <summary>
        /// 内部发布方法（使用异步 API）
        /// </summary>
        private async Task PublishInternalAsync<TMessage>(TMessage message, int delayMilliseconds, bool isDelay)
        {
            var messageType = typeof(TMessage);
            
            // 从缓存中获取路由信息（避免重复反射）
            var routeInfo = routeCache.GetRouteInfo(messageType);

            if (routeInfo == null || 
                string.IsNullOrEmpty(routeInfo.ExchangeName) || 
                string.IsNullOrEmpty(routeInfo.RouteKey))
            {
                throw new InvalidOperationException(
                    $"消息类型 {messageType.Name} 必须关联一个标注了 [Consumer] 特性的消费者类");
            }

            // 每次发布都创建新的 Channel（推荐做法）
            IChannel channel = null;
            try
            {
                // 创建 Channel
                channel = await connectionFactory.CreateChannelAsync();

                // 使用注入的序列化器
                var body = serializer.Serialize(message);

                // 创建消息属性
                var properties = new BasicProperties
                {
                    Persistent = true, // 持久化消息
                    ContentType = serializer.ContentType,
                    ContentEncoding = "utf-8",
                    Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                    MessageId = Guid.NewGuid().ToString(),
                    Type = messageType.FullName
                };

                // 如果是延时消息，添加延时头
                if (isDelay)
                {
                    properties.Headers = new Dictionary<string, object>
                    {
                        { "x-delay", delayMilliseconds }
                    };
                }

                // 使用异步 API 发布消息
                await channel.BasicPublishAsync(
                    exchange: routeInfo.ExchangeName,
                    routingKey: routeInfo.RouteKey,
                    mandatory: false,
                    basicProperties: properties,
                    body: body);

                if (isDelay)
                {
                    logger.LogInformation(
                        "成功发布延时消息 ({Delay}ms) 到 Exchange: {Exchange}, RouteKey: {RouteKey}, MessageType: {Type}",
                        delayMilliseconds, routeInfo.ExchangeName, routeInfo.RouteKey, messageType.Name);
                }
                else
                {
                    logger.LogInformation(
                        "成功发布消息到 Exchange: {Exchange}, RouteKey: {RouteKey}, MessageType: {Type}",
                        routeInfo.ExchangeName, routeInfo.RouteKey, messageType.Name);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "发布消息失败 - Exchange: {Exchange}, RouteKey: {RouteKey}, MessageType: {Type}",
                    routeInfo?.ExchangeName, routeInfo?.RouteKey, messageType.Name);
                throw;
            }
            finally
            {
                // 使用完后关闭 Channel
                if (channel != null)
                {
                    try
                    {
                        await channel.CloseAsync();
                        channel.Dispose();
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "关闭 Channel 时发生错误");
                    }
                }
            }
        }
    }
}
