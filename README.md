# SpeakEase.RabbitMq

一个基于 .NET 9.0 的 RabbitMQ 轻量级封装库，使用 Source Generator 技术实现自动依赖注入和消费者订阅。

## ✨ 特性

- 🚀 **自动代码生成** - 使用 Source Generator 自动生成依赖注入和订阅代码
- 🎯 **特性驱动** - 通过特性标注实现配置和依赖注入
- 📦 **轻量级封装** - 简化 RabbitMQ 的使用复杂度
- 🔧 **灵活配置** - 支持 JSON 配置文件和代码配置
- 💉 **自动注入** - 支持 Singleton、Scoped、Transient 三种生命周期
- 🎨 **职责分离** - 消息配置与消费逻辑分离

## 📦 项目结构

```
SpeakEase.RabbitMq/
├── SpeakEase.RabbitMq/              # 核心库
│   ├── Attributes/                   # 特性定义
│   │   ├── ConsumerAttribute.cs      # 消费者配置特性
│   │   ├── InjectAttribute.cs        # 依赖注入特性
│   │   └── ServiceLifetime.cs        # 服务生命周期枚举
│   ├── Connection/                   # 连接管理
│   ├── Interface/                    # 接口定义
│   ├── Serialization/                # 序列化
│   └── Extensions/                   # 扩展方法
├── SpeakEase.RabbitMq.Sourcegenerate/ # Source Generator
│   ├── ConsumerSourceGenerator.cs    # 消费者订阅代码生成器
│   └── InjectSourceGenerator.cs      # 依赖注入代码生成器
├── SpeakEase.RabbitMq.Consumer/      # 消费者示例
└── SpeakEase.Rabbitmq.Product/       # 生产者示例
```

## 🚀 快速开始

### 1️⃣ 安装依赖

```bash
dotnet add package RabbitMQ.Client
dotnet add package Microsoft.Extensions.Hosting
```

### 2️⃣ 定义消息类

在消息类上使用 `[Consumer]` 特性配置队列信息：

```csharp
using SpeakEase.RabbitMq.Attributes;

[Consumer(
    ExchangeName = "SpeakEase.MQ",
    QueueName = "SpeakEase",
    RouteKey = "SpeakEase.Test")]
public class TestMessage
{
    public Guid MessageId { get; set; }
    public string Name { get; set; }
}
```

### 3️⃣ 实现消费者

实现 `IConsumer<T>` 接口并使用 `[Inject]` 特性指定生命周期：

```csharp
using SpeakEase.RabbitMq.Attributes;
using SpeakEase.RabbitMq.Interface;

[Inject(ServiceLifetime = ServiceLifetime.Transient)]
internal class TestConsumer : IConsumer<TestMessage>
{
    private readonly ILogger<TestConsumer> _logger;

    public TestConsumer(ILogger<TestConsumer> logger)
    {
        _logger = logger;
    }

    public Task HandleAsync(TestMessage message, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"处理消息: {message.MessageId} - {message.Name}");
        return Task.CompletedTask;
    }

    public Task FailureAsync(TestMessage message, CancellationToken cancellationToken = default)
    {
        _logger.LogError($"处理失败: {message.MessageId}");
        return Task.CompletedTask;
    }
}
```

### 4️⃣ 配置服务

创建 `Configurations/rabbitmq.json`：

```json
{
  "RabbitMqOptions": {
    "HostName": "localhost",
    "Port": 5672,
    "UserName": "guest",
    "Password": "guest",
    "VirtualHost": "/",
    "AutomaticRecoveryEnabled": true,
    "NetworkRecoveryInterval": 5,
    "RequestedHeartbeat": 60
  }
}
```

在 `Program.cs` 中注册服务：

```csharp
using SpeakEase.RabbitMq.Extensions;

var host = Host.CreateDefaultBuilder()
    .ConfigureHostConfiguration(cfg =>
    {
        cfg.AddJsonFile("Configurations/rabbitmq.json");
    })
    .ConfigureServices((context, services) =>
    {
        services.AddRabbitMq(context.Configuration);
        services.AddInjectServices(); // 自动注册所有带 [Inject] 特性的服务
    })
    .Build();

// 订阅所有消费者
await host.Services.SubscribeAllConsumersAsync();

await host.RunAsync();
```

## 🎯 核心概念

### 特性说明

#### `[Consumer]` 特性
标注在**消息类**上，定义队列配置：
- `ExchangeName` - 交换机名称
- `QueueName` - 队列名称
- `RouteKey` - 路由键

#### `[Inject]` 特性
标注在**消费者类**上，定义依赖注入生命周期：
- `ServiceLifetime.Singleton` - 单例模式
- `ServiceLifetime.Scoped` - 作用域模式（默认）
- `ServiceLifetime.Transient` - 瞬态模式

### Source Generator 工作原理

#### InjectSourceGenerator
自动扫描所有带 `[Inject]` 特性的类，生成：
```csharp
public static IServiceCollection AddInjectServices(this IServiceCollection services)
{
    services.AddTransient<IConsumer<TestMessage>, TestConsumer>();
    return services;
}
```

#### ConsumerSourceGenerator
自动扫描所有实现 `IConsumer<T>` 的类，并检查消息类型 `T` 是否有 `[Consumer]` 特性，生成：
```csharp
public static async Task SubscribeAllConsumersAsync(this IServiceProvider serviceProvider)
{
    var consumerFactory = serviceProvider.GetRequiredService<IConsumerFactory>();
    await consumerFactory.SubscribeAsync<TestMessage>();
}
```

## 📝 使用示例

### 发送消息

```csharp
public class MessageProducer
{
    private readonly IMessagePublish _messagePublish;

    public MessageProducer(IMessagePublish messagePublish)
    {
        _messagePublish = messagePublish;
    }

    public async Task SendMessageAsync()
    {
        var message = new TestMessage
        {
            MessageId = Guid.NewGuid(),
            Name = "Hello RabbitMQ"
        };

        await _messagePublish.PublishAsync(message);
    }
}
```

### 多个消费者

```csharp
// 消息类 1
[Consumer(ExchangeName = "order.exchange", QueueName = "order.queue", RouteKey = "order.create")]
public class OrderMessage
{
    public int OrderId { get; set; }
}

// 消息类 2
[Consumer(ExchangeName = "log.exchange", QueueName = "log.queue", RouteKey = "log.info")]
public class LogMessage
{
    public string Content { get; set; }
}

// 消费者 1
[Inject(ServiceLifetime = ServiceLifetime.Scoped)]
public class OrderConsumer : IConsumer<OrderMessage>
{
    public Task HandleAsync(OrderMessage message, CancellationToken cancellationToken)
    {
        // 处理订单消息
        return Task.CompletedTask;
    }

    public Task FailureAsync(OrderMessage message, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

// 消费者 2
[Inject(ServiceLifetime = ServiceLifetime.Singleton)]
public class LogConsumer : IConsumer<LogMessage>
{
    public Task HandleAsync(LogMessage message, CancellationToken cancellationToken)
    {
        // 处理日志消息
        return Task.CompletedTask;
    }

    public Task FailureAsync(LogMessage message, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
```

Source Generator 会自动生成所有消费者的注册和订阅代码！

## 🔧 配置选项

### RabbitMqOptions

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| HostName | string | localhost | RabbitMQ 服务器地址 |
| Port | int | 5672 | 端口号 |
| UserName | string | guest | 用户名 |
| Password | string | guest | 密码 |
| VirtualHost | string | / | 虚拟主机 |
| AutomaticRecoveryEnabled | bool | true | 自动重连 |
| NetworkRecoveryInterval | int | 5 | 网络恢复间隔（秒） |
| RequestedHeartbeat | ushort | 60 | 心跳间隔（秒） |

## 🛠️ 高级用法

### 手动配置

```csharp
services.AddRabbitMq(options =>
{
    options.HostName = "localhost";
    options.Port = 5672;
    options.UserName = "admin";
    options.Password = "admin";
});
```

### 禁用 Channel 池

```csharp
services.AddRabbitMq(configuration, useChannelPool: false);
```

### 查看生成的代码

```bash
# 生成代码到 obj/GeneratedDebug 目录
dotnet build /p:EmitCompilerGeneratedFiles=true /p:CompilerGeneratedFilesOutputPath=obj/GeneratedDebug

# 查看完后清理
dotnet clean
```

## ⚠️ 注意事项

1. **消息类配置** - `[Consumer]` 特性应标注在消息类上，而不是消费者类
2. **生命周期选择**：
   - `Transient` - 适合无状态、轻量级的消费者
   - `Scoped` - 适合大多数场景（默认）
   - `Singleton` - 适合无状态且线程安全的消费者，注意并发安全
3. **Source Generator 文件** - `Generated/` 目录已在 `.gitignore` 中，不要手动提交

## 📚 技术栈

- .NET 9.0
- RabbitMQ.Client 6.8+
- Microsoft.CodeAnalysis (Roslyn)
- Source Generators

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 License

MIT License

## 🔗 相关链接

- [RabbitMQ 官方文档](https://www.rabbitmq.com/documentation.html)
- [Source Generators 文档](https://learn.microsoft.com/en-us/dotnet/csharp/roslyn-sdk/source-generators-overview)
