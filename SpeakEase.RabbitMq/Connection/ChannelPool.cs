using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System.Collections.Concurrent;

namespace SpeakEase.RabbitMq.Connection
{
    /// <summary>
    /// Channel 池接口
    /// </summary>
    public interface IChannelPool : IDisposable
    {
        /// <summary>
        /// 获取一个 Channel（从池中获取或创建新的）
        /// </summary>
        Task<IChannelAccessor> AcquireAsync(string channelName = null);
    }

    /// <summary>
    /// Channel 访问器，用于自动归还 Channel 到池中
    /// </summary>
    public interface IChannelAccessor : IDisposable
    {
        /// <summary>
        /// 获取 Channel
        /// </summary>
        IChannel Channel { get; }

        /// <summary>
        /// Channel 名称
        /// </summary>
        string Name { get; }
    }

    /// <summary>
    /// Channel 池实现
    /// 参考 ABP 框架的对象池模式
    /// </summary>
    public class ChannelPool(
        IRabbitMqConnectionFactory connectionFactory,
        ILogger<ChannelPool> logger) : IChannelPool
    {
        private readonly ConcurrentDictionary<string, ConcurrentBag<IChannel>> _pools = new ConcurrentDictionary<string, ConcurrentBag<IChannel>>();
        private readonly SemaphoreSlim _poolLock = new SemaphoreSlim(1, 1);
        private bool _disposed = false;

        /// <summary>
        /// 每个池的最大大小
        /// </summary>
        public int MaxPoolSize { get; set; } = 100;

        /// <summary>
        /// 获取一个 Channel
        /// </summary>
        public async Task<IChannelAccessor> AcquireAsync(string channelName = null)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(ChannelPool));
            }

            channelName ??= "default";

            var pool = _pools.GetOrAdd(channelName, _ => new ConcurrentBag<IChannel>());

            // 尝试从池中获取可用的 Channel
            while (pool.TryTake(out var channel))
            {
                if (channel.IsOpen)
                {
                    logger.LogDebug("从池中获取 Channel - Name: {Name}, ChannelNumber: {Number}", 
                        channelName, channel.ChannelNumber);
                    return new ChannelAccessor(channel, channelName, this);
                }

                // Channel 已关闭，释放它
                try
                {
                    channel.Dispose();
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "释放已关闭的 Channel 时发生错误");
                }
            }

            // 池中没有可用的 Channel，创建新的
            try
            {
                var newChannel = await connectionFactory.CreateChannelAsync();
                logger.LogDebug("创建新 Channel - Name: {Name}, ChannelNumber: {Number}", 
                    channelName, newChannel.ChannelNumber);
                return new ChannelAccessor(newChannel, channelName, this);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "创建新 Channel 失败 - Name: {Name}", channelName);
                throw;
            }
        }

        /// <summary>
        /// 归还 Channel 到池中
        /// </summary>
        internal void Return(IChannel channel, string channelName)
        {
            if (_disposed || channel == null || !channel.IsOpen)
            {
                channel?.Dispose();
                return;
            }

            var pool = _pools.GetOrAdd(channelName, _ => new ConcurrentBag<IChannel>());

            // 检查池大小
            if (pool.Count >= MaxPoolSize)
            {
                logger.LogDebug("Channel 池已满，关闭 Channel - Name: {Name}, ChannelNumber: {Number}",
                    channelName, channel.ChannelNumber);
                try
                {
                    channel.CloseAsync().GetAwaiter().GetResult();
                    channel.Dispose();
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "关闭超出池大小的 Channel 时发生错误");
                }
                return;
            }

            pool.Add(channel);
            logger.LogDebug("归还 Channel 到池中 - Name: {Name}, ChannelNumber: {Number}, PoolSize: {Size}",
                channelName, channel.ChannelNumber, pool.Count);
        }

        /// <summary>
        /// 释放所有资源
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                _poolLock.Wait();
                try
                {
                    foreach (var poolPair in _pools)
                    {
                        var pool = poolPair.Value;
                        while (pool.TryTake(out var channel))
                        {
                            try
                            {
                                if (channel.IsOpen)
                                {
                                    channel.CloseAsync().GetAwaiter().GetResult();
                                }
                                channel.Dispose();
                            }
                            catch (Exception ex)
                            {
                                logger.LogWarning(ex, "释放池中的 Channel 时发生错误");
                            }
                        }
                    }

                    _pools.Clear();
                    logger.LogInformation("Channel 池已释放");
                }
                finally
                {
                    _poolLock.Release();
                }

                _poolLock?.Dispose();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "释放 Channel 池时发生错误");
            }
        }

        /// <summary>
        /// Channel 访问器实现
        /// </summary>
        private class ChannelAccessor : IChannelAccessor
        {
            private readonly ChannelPool _pool;
            private bool _disposed = false;

            public IChannel Channel { get; }
            public string Name { get; }

            public ChannelAccessor(IChannel channel, string name, ChannelPool pool)
            {
                Channel = channel;
                Name = name;
                _pool = pool;
            }

            public void Dispose()
            {
                if (_disposed)
                    return;

                _disposed = true;

                // 归还 Channel 到池中
                _pool.Return(Channel, Name);
            }
        }
    }
}
