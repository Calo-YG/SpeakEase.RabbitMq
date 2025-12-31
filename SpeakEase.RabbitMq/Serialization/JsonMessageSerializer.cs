using System.Text;
using System.Text.Json;

namespace SpeakEase.RabbitMq.Serialization
{
    /// <summary>
    /// JSON 消息序列化器实现
    /// </summary>
    public class JsonMessageSerializer : IMessageSerializer
    {
        private readonly JsonSerializerOptions _options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        public string ContentType => "application/json";

        /// <summary>
        /// 序列化消息
        /// </summary>
        public byte[] Serialize<TMessage>(TMessage message)
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            var json = JsonSerializer.Serialize(message, _options);
            return Encoding.UTF8.GetBytes(json);
        }

        /// <summary>
        /// 反序列化消息
        /// </summary>
        public TMessage Deserialize<TMessage>(byte[] data)
        {
            if (data == null || data.Length == 0)
                throw new ArgumentException("Data cannot be null or empty", nameof(data));

            var json = Encoding.UTF8.GetString(data);
            return JsonSerializer.Deserialize<TMessage>(json, _options);
        }
    }
}
