using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using SpeakEase.RabbitMq.Configuration;

namespace SpeakEase.RabbitMq.Connection
{
    /// <summary>
    /// RabbitMQ 连接工厂接口
    /// </summary>
    public interface IRabbitMqConnectionFactory
    {
        /// <summary>
        /// 获取或创建连接
        /// </summary>
        Task<IConnection> GetOrCreateConnectionAsync();

        /// <summary>
        /// 创建 Channel
        /// </summary>
        Task<IChannel> CreateChannelAsync();
    }

    /// <summary>
    /// RabbitMQ 连接工厂实现
    /// 参考 ABP 框架的连接池管理模式
    /// </summary>
    public class RabbitMqConnectionFactory(
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqConnectionFactory> logger) : IRabbitMqConnectionFactory, IDisposable
    {
        private readonly RabbitMqOptions _options = options.Value;
        private IConnection _connection;
        private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1, 1);
        private bool _disposed = false;

        /// <summary>
        /// 获取或创建连接（线程安全）
        /// </summary>
        public async Task<IConnection> GetOrCreateConnectionAsync()
        {
            if (_connection != null && _connection.IsOpen)
            {
                return _connection;
            }

            await _connectionLock.WaitAsync();
            try
            {
                // 双重检查锁定模式
                if (_connection != null && _connection.IsOpen)
                {
                    return _connection;
                }

                // 如果连接存在但已关闭，先释放
                if (_connection != null)
                {
                    try
                    {
                        await _connection.CloseAsync();
                        _connection.Dispose();
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "关闭旧连接时发生错误");
                    }
                }

                _connection = await CreateConnectionAsync();
                return _connection;
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        /// <summary>
        /// 创建新的 RabbitMQ 连接
        /// </summary>
        private async Task<IConnection> CreateConnectionAsync()
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = _options.HostName,
                    Port = _options.Port,
                    UserName = _options.UserName,
                    Password = _options.Password,
                    VirtualHost = _options.VirtualHost,
                    AutomaticRecoveryEnabled = _options.AutomaticRecoveryEnabled,
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(_options.NetworkRecoveryInterval),
                    RequestedHeartbeat = TimeSpan.FromSeconds(_options.RequestedHeartbeat)
                };

                // 使用异步 API 创建连接
                var connection = await factory.CreateConnectionAsync();

                logger.LogInformation(
                    "成功创建 RabbitMQ 连接 - Host: {HostName}:{Port}, VirtualHost: {VirtualHost}",
                    _options.HostName, _options.Port, _options.VirtualHost);

                return connection;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, 
                    "创建 RabbitMQ 连接失败 - Host: {HostName}:{Port}", 
                    _options.HostName, _options.Port);
                throw;
            }
        }

        /// <summary>
        /// 创建 Channel（每次都创建新的）
        /// </summary>
        public async Task<IChannel> CreateChannelAsync()
        {
            var connection = await GetOrCreateConnectionAsync();

            try
            {
                var channel = await connection.CreateChannelAsync();
                
                logger.LogDebug("成功创建 RabbitMQ Channel - ChannelNumber: {ChannelNumber}", channel.ChannelNumber);
                
                return channel;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "创建 RabbitMQ Channel 失败");
                throw;
            }
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            try
            {
                _connectionLock.Wait();
                try
                {
                    if (_connection != null)
                    {
                        if (_connection.IsOpen)
                        {
                            _connection.CloseAsync().GetAwaiter().GetResult();
                        }
                        _connection.Dispose();
                        _connection = null;
                        logger.LogInformation("RabbitMQ 连接已释放");
                    }
                }
                finally
                {
                    _connectionLock.Release();
                }

                _connectionLock?.Dispose();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "释放 RabbitMQ 连接时发生错误");
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}
