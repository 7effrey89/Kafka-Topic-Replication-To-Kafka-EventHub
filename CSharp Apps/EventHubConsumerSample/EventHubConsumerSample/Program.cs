using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace EventHub_Consumer
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            const string eventHubNameSpace = "jlaEventHubCloneSource"; 
            const string topic = "topica-clone"; //Topic name to host the cloned topic
            const string sasToken = "azHBQNu71yJV0ujCppWGbByZ0bH6d+a+16GbQ4+8yh8="; // SAS token

            var cConfig = new ConsumerConfig
            {
                BootstrapServers = eventHubNameSpace + ".servicebus.windows.net:9093",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = "$ConnectionString",
                SaslPassword = $"Endpoint=sb://{eventHubNameSpace}.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={sasToken}", //EventHub NameSpace Key1
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(cConfig).Build())
            {
                c.Subscribe(topic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    string brokerList = cConfig.BootstrapServers.ToString();
                    Console.WriteLine("Start consuming message/s from topic: " + topic + ", broker(s): " + brokerList);
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Close and Release all the resources held by this consumer  
                    c.Close();
                }
            }
        }
        /*
         * https://www.thecodebuzz.com/apache-kafka-net-client-producer-consumer-csharp-confluent-examples-ii/
         */
    }
}