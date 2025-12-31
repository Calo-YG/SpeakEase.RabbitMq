namespace SpeakEase.RabbitMq.Configuration
{
    /// <summary>
    /// RabbitMQ 配置选项
    /// </summary>
    public class RabbitMqOptions
    {
        /// <summary>
        /// 主机地址
        /// </summary>
        public string HostName { get; set; }

        /// <summary>
        /// 端口
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// 用户名
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        /// 密码
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// 虚拟主机
        /// </summary>
        public string VirtualHost { get; set; }

        /// <summary>
        /// 自动重连
        /// </summary>
        public bool AutomaticRecoveryEnabled { get; set; } = true;

        /// <summary>
        /// 网络恢复间隔（秒）
        /// </summary>
        public int NetworkRecoveryInterval { get; set; } = 5;

        /// <summary>
        /// 请求的心跳间隔（秒）
        /// </summary>
        public ushort RequestedHeartbeat { get; set; } = 60;
    }
}
