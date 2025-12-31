namespace SpeakEase.RabbitMq.Attributes
{
    /// <summary>
    /// 服务生命周期枚举
    /// </summary>
    public enum ServiceLifetime
    {
        /// <summary>
        /// 单例模式 - 整个应用程序生命周期内只创建一个实例
        /// </summary>
        Singleton = 0,

        /// <summary>
        /// 作用域模式 - 每个作用域（如每个HTTP请求）创建一个实例（默认）
        /// </summary>
        Scoped = 1,

        /// <summary>
        /// 瞬态模式 - 每次请求都创建一个新实例
        /// </summary>
        Transient = 2
    }
}
