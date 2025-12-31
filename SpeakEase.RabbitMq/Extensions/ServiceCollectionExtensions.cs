using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using SpeakEase.RabbitMq.Cache;
using SpeakEase.RabbitMq.Configuration;
using SpeakEase.RabbitMq.Connection;
using SpeakEase.RabbitMq.Interface;
using SpeakEase.RabbitMq.Serialization;

namespace SpeakEase.RabbitMq.Extensions
{
    /// <summary>
    /// 服务集合扩展方法
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// 添加 RabbitMQ 服务
        /// </summary>
        /// <param name="services">服务集合</param>
        /// <param name="configureOptions">配置选项</param>
        /// <param name="useChannelPool">是否使用 Channel 池（默认：true）</param>
        public static IServiceCollection AddRabbitMq(
            this IServiceCollection services,
            Action<RabbitMqOptions> configureOptions,
            bool useChannelPool = true)
        {
            if (services == null)
                throw new ArgumentNullException(nameof(services));

            if (configureOptions == null)
                throw new ArgumentNullException(nameof(configureOptions));

            // 注册配置
            services.Configure(configureOptions);

            // 注册连接工厂（单例）
            services.TryAddSingleton<IRabbitMqConnectionFactory, RabbitMqConnectionFactory>();

            // 注册 Channel 池（单例）
            if (useChannelPool)
            {
                services.TryAddSingleton<IChannelPool, ChannelPool>();
            }

            // 注册消费者路由缓存（单例）
            services.TryAddSingleton<IConsumerRouteCache, ConsumerRouteCache>();

            // 注册消息序列化器（单例）
            services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();

            //注册消费工厂
            services.TryAddSingleton<IConsumerFactory, ConsumerFactory>();

            // 注册消息发布器（单例）
            services.TryAddSingleton<IMessagePublish, MessagePublish>();

            return services;
        }


        public static IServiceCollection AddRabbitMq(this IServiceCollection services,IConfiguration configuration,bool useChannelPool = true)
        {
            if (services == null)
                throw new ArgumentNullException(nameof(services));

            services.Configure<RabbitMqOptions>(configuration.GetSection(nameof(RabbitMqOptions)));

            // 注册连接工厂（单例）
            services.TryAddSingleton<IRabbitMqConnectionFactory, RabbitMqConnectionFactory>();

            // 注册 Channel 池（单例）
            if (useChannelPool)
            {
                services.TryAddSingleton<IChannelPool, ChannelPool>();
            }

            // 注册消费者路由缓存（单例）
            services.TryAddSingleton<IConsumerRouteCache, ConsumerRouteCache>();

            // 注册消息序列化器（单例）
            services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();

            // 注册消息发布器（单例）
            services.TryAddSingleton<IMessagePublish, MessagePublish>();

            //注册消费工厂
            services.TryAddSingleton<IConsumerFactory, ConsumerFactory>();

            return services;

        }
    }
}
