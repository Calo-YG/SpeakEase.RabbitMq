using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SpeakEase.RabbitMq.Cache;
using SpeakEase.RabbitMq.Connection;
using SpeakEase.RabbitMq.Interface;
using SpeakEase.RabbitMq.Serialization;
using System.Collections.Concurrent;

namespace SpeakEase.RabbitMq
{
    /// <summary>
    /// 消费者工厂实现
    /// 负责管理 RabbitMQ 消费者的订阅和取消订阅
    /// </summary>
    internal class ConsumerFactory(
        IConsumerRouteCache consumerRouteCache,
        IServiceProvider serviceProvider,
        IRabbitMqConnectionFactory rabbitMqConnectionFactory,
        IMessageSerializer messageSerializer,
        ILogger<ConsumerFactory> logger) : IConsumerFactory, IDisposable
    {

        // 存储消费者的 Channel 和 ConsumerTag，用于取消订阅
        private readonly ConcurrentDictionary<Type, ConsumerSubscription> _subscriptions = new();
        private bool _disposed = false;

        /// <summary>
        /// 订阅消息
        /// </summary>
        public async Task SubscribeAsync<TMessage>()
        {
            var messageType = typeof(TMessage);

            // 检查是否已经订阅
            if (_subscriptions.ContainsKey(messageType))
            {
                logger.LogWarning("消息类型 {MessageType} 已经订阅，跳过重复订阅", messageType.Name);
                return;
            }

            // 获取路由信息
            var routeInfo = consumerRouteCache.GetRouteInfo(messageType);
            if (routeInfo == null)
            {
                throw new InvalidOperationException($"消息类型 {messageType.Name} 必须标注 [Consumer] 特性");
            }

            try
            {
                // 创建 Channel
                var channel = await rabbitMqConnectionFactory.CreateChannelAsync();

                // 声明交换机（幂等操作）
                await channel.ExchangeDeclareAsync(
                    exchange: routeInfo.ExchangeName,
                    type: ExchangeType.Direct,
                    durable: true,
                    autoDelete: false);

                // 声明队列（幂等操作）
                await channel.QueueDeclareAsync(
                    queue: routeInfo.QueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false);

                // 绑定队列到交换机
                await channel.QueueBindAsync(
                    queue: routeInfo.QueueName,
                    exchange: routeInfo.ExchangeName,
                    routingKey: routeInfo.RouteKey);

                // 设置 QoS（一次只处理一条消息）
                await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

                // 创建异步事件消费者
                var consumer = new AsyncEventingBasicConsumer(channel);
                
                // 订阅消息接收事件
                consumer.ReceivedAsync += async (sender, ea) =>
                {
                    await HandleMessageAsync<TMessage>(channel, ea);
                };

                // 开始消费
                var consumerTag = await channel.BasicConsumeAsync(
                    queue: routeInfo.QueueName,
                    autoAck: false, // 手动确认
                    consumer: consumer);

                // 保存订阅信息
                var subscription = new ConsumerSubscription
                {
                    Channel = channel,
                    ConsumerTag = consumerTag,
                    Consumer = consumer,
                    RouteInfo = routeInfo
                };

                _subscriptions.TryAdd(messageType, subscription);

                logger.LogInformation(
                    "成功订阅消息 - MessageType: {MessageType}, Queue: {Queue}, Exchange: {Exchange}, RouteKey: {RouteKey}",
                    messageType.Name, routeInfo.QueueName, routeInfo.ExchangeName, routeInfo.RouteKey);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "订阅消息失败 - MessageType: {MessageType}", messageType.Name);
                throw;
            }
        }

        /// <summary>
        /// 取消订阅消息
        /// </summary>
        public async Task UnsubscribeAsync<TMessage>()
        {
            var messageType = typeof(TMessage);

            if (!_subscriptions.TryRemove(messageType, out var subscription))
            {
                logger.LogWarning("消息类型 {MessageType} 未订阅，无需取消", messageType.Name);
                return;
            }

            try
            {
                // 取消消费
                if (subscription.Channel.IsOpen)
                {
                    await subscription.Channel.BasicCancelAsync(subscription.ConsumerTag);
                }

                // 关闭 Channel
                await subscription.Channel.CloseAsync();

                subscription.Channel.Dispose();

                logger.LogInformation(
                    "成功取消订阅 - MessageType: {MessageType}, Queue: {Queue}",
                    messageType.Name, subscription.RouteInfo.QueueName);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "取消订阅失败 - MessageType: {MessageType}", messageType.Name);
                throw;
            }
        }

        /// <summary>
        /// 处理接收到的消息
        /// </summary>
        private async Task HandleMessageAsync<TMessage>(IChannel channel, BasicDeliverEventArgs ea)
        {
            var messageType = typeof(TMessage);

            // 反序列化消息
            var message = messageSerializer.Deserialize<TMessage>(ea.Body.ToArray());
            // 从 DI 容器中获取消费者实例（使用 Scope）
            using var scope = serviceProvider.CreateScope();
            var consumer = scope.ServiceProvider.GetRequiredService<IConsumer<TMessage>>();

            try
            {
                // 调用消费者处理消息
                await consumer.HandleAsync(message, CancellationToken.None);

                logger.LogDebug(
                    "成功处理消息 - MessageType: {MessageType}, MessageId: {MessageId}",
                    messageType.Name, ea.BasicProperties?.MessageId);
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "处理消息失败 - MessageType: {MessageType}, MessageId: {MessageId}",
                    messageType.Name, ea.BasicProperties?.MessageId);

                // 消息处理失败，拒绝消息并重新入队
                // 注意：这里可以根据实际需求决定是否重新入队（requeue: true/false）
                //await channel.BasicNackAsync(
                //    deliveryTag: ea.DeliveryTag,
                //    multiple: false,
                //    requeue: true); // true: 重新入队，false: 丢弃或进入死信队列

                await consumer.FailureAsync(message,CancellationToken.None);
            }
            finally
            {
                // 手动确认消息
                await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
            }
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                // 取消所有订阅
                foreach (var subscription in _subscriptions.Values)
                {
                    try
                    {
                        if (subscription.Channel.IsOpen)
                        {
                            subscription.Channel.BasicCancelAsync(subscription.ConsumerTag)
                                .GetAwaiter().GetResult();
                            subscription.Channel.CloseAsync().GetAwaiter().GetResult();
                        }
                        subscription.Channel.Dispose();
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "释放订阅资源时发生错误");
                    }
                }

                _subscriptions.Clear();
                logger.LogInformation("ConsumerFactory 已释放所有资源");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "释放 ConsumerFactory 资源时发生错误");
            }
        }

        /// <summary>
        /// 消费者订阅信息
        /// </summary>
        private class ConsumerSubscription
        {
            public IChannel Channel { get; set; }
            public string ConsumerTag { get; set; }
            public AsyncEventingBasicConsumer Consumer { get; set; }
            public ConsumerRouteInfo RouteInfo { get; set; }
        }
    }
}
