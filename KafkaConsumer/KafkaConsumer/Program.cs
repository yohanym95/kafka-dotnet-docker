using Confluent.Kafka;
using System;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "order-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};
using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
consumer.Subscribe("demo-topic");
Console.WriteLine("Waiting for events...");
try
{
    while (true)
    {
        var result = consumer.Consume();
        Console.WriteLine($"Received: {result.Message.Value}");
    }
}
catch (Exception ex)
{
    Console.WriteLine(ex.Message);
}
finally
{
    consumer.Close();
}