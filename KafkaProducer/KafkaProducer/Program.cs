using Confluent.Kafka;
using System;
using System.Threading.Tasks;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};
using var producer = new ProducerBuilder<Null, string>(config).Build();
Console.WriteLine("Sending order event...");
try
{
    var result = await producer.ProduceAsync(
        "demo-topic",
        new Message<Null, string>
        {
            Value = "Order Created"
        });
    Console.WriteLine($"Sent: {result.TopicPartitionOffset}");
}
catch (Exception ex)
{
    Console.WriteLine(ex.Message);
}