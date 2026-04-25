using Confluent.Kafka;
using System;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "order-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false // IMPORTANT: disable auto commit
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
consumer.Subscribe("demo-topic");

Console.WriteLine("Waiting for events...");

try
{
    while (true)
    {
        var result = consumer.Consume();

        try
        {
            // PROCESS MESSAGE
            Console.WriteLine($"Received: {result.Message.Value}");

            // process message as we required

            // COMMIT OFFSET after successful processing
            consumer.Commit(result);
        }
        catch (Exception processingEx)
        {
            Console.WriteLine($"Processing failed: {processingEx.Message}");

            // DO NOT commit → message will be retried
        }
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