using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kafka_Consumer
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            const string eventHubNameSpace = "jlaKafkaOrgSource";
            const string topic = "topica";//Topic name 
            const string sasToken = "Y5BB6HzpdjUVsftKEkRwDLLA/kVWhnNYjxJf1XuOoOI=+a+16GbQ4+8yh8="; // SAS token

            var cConfig = new ConsumerConfig
            {
                /* 
                //Kafka Config Template
                BootstrapServers = "xxxxx",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = "xxxxxxx",
                SaslPassword = "xxxxx+",
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest

                //EventHub Config Template
                bootstrap.servers=NAMESPACENAME.servicebus.windows.net:9093
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{YOUR.EVENTHUBS.CONNECTION.STRING}";
                */

                BootstrapServers = eventHubNameSpace + ".servicebus.windows.net:9093",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = "$ConnectionString",
                SaslPassword = $"Endpoint=sb://{eventHubNameSpace}.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={eventHubNameSpace}", //EventHub NameSpace Key1
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