using SpeakEase.RabbitMq.Attributes;
using System.Collections.Concurrent;

namespace SpeakEase.RabbitMq.Cache
{
    /// <summary>
    /// 消费者路由信息
    /// </summary>
    public class ConsumerRouteInfo
    {
        public string ExchangeName { get; set; }
        public string QueueName { get; set; }
        public string RouteKey { get; set; }
    }

    /// <summary>
    /// 消费者路由缓存接口
    /// </summary>
    public interface IConsumerRouteCache
    {
        /// <summary>
        /// 获取消息类型对应的路由信息
        /// </summary>
        ConsumerRouteInfo GetRouteInfo(Type messageType);
    }

    /// <summary>
    /// 消费者路由缓存实现
    /// 使用线程安全字典缓存反射结果，避免重复反射
    /// </summary>
    public class ConsumerRouteCache : IConsumerRouteCache
    {
        private readonly ConcurrentDictionary<Type, ConsumerRouteInfo> _routeCache;

        public ConsumerRouteCache()
        {
            _routeCache = new ConcurrentDictionary<Type, ConsumerRouteInfo>();
        }

        /// <summary>
        /// 获取消息类型对应的路由信息（带缓存）
        /// 直接从消息类型反射获取 ConsumerAttribute
        /// </summary>
        public ConsumerRouteInfo GetRouteInfo(Type messageType)
        {
            if (messageType == null)
                throw new ArgumentNullException(nameof(messageType));

            // 从缓存中获取，如果不存在则通过反射创建
            return _routeCache.GetOrAdd(messageType, type =>
            {
                // 直接从消息类型获取 ConsumerAttribute
                var attribute = type
                    .GetCustomAttributes(typeof(ConsumerAttribute), false)
                    .FirstOrDefault() as ConsumerAttribute;

                if (attribute == null)
                    return null;

                return new ConsumerRouteInfo
                {
                    ExchangeName = attribute.ExchangeName,
                    QueueName = attribute.QueueName,
                    RouteKey = attribute.RouteKey
                };
            });
        }
    }
}
