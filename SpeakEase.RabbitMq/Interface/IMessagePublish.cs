
namespace SpeakEase.RabbitMq.Interface
{
    public interface IMessagePublish
    {
        /// <summary>
        /// 发布消息
        /// </summary>
        /// <typeparam name="Temessage"></typeparam>
        /// <param name="message"></param>
        /// <returns></returns>
        public Task PublishAsync<Temessage>(Temessage message);

        /// <summary>
        /// 发布延时消息
        /// </summary>
        /// <typeparam name="Temessage"></typeparam>
        /// <param name="message"></param>
        /// <returns></returns>
        public Task PublishDelayAsync<Temessage>(Temessage message);
    }
}
