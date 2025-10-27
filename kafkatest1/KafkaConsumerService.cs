using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

public class KafkaConsumerService : BackgroundService
{
    private readonly string _bootstrapServers = "localhost:29092,39092,49092";
    private readonly string _topic = "test2";
    private readonly string _groupId = "kafkatest1-consumer-group";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _ = Task.Run(async () =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(_topic);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);
                    if (result != null)
                    {
                        // 處理收到的訊息
                        Console.WriteLine($"Key: {result.Message.Key}, Value: {result.Message.Value}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Kafka consume error: {ex.Message}");
                }
            }

            consumer.Close();
        });
    }
}
