using System;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Confluent.Kafka;
using MyTelemetryClasses;
using Newtonsoft.Json;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Kafka_Producer
{
    class Program
    {
        
        public static async Task Main(string[] args)
        {
            //Kafka settings
            const string eventHubNameSpace = "jlaKafkaOrgSource";
            const string topic = "topica";
            const string sasToken = "Y5BB6HzpdjUVsftKEkRwDLLA/kVWhnNYjxJf1XuOoOI=";

            //demo settings
            int numberOfEventsSent = 10;
            bool dumpJsonOutput = true;
            bool useJsonAsEventFormat = true;

            var config = new ProducerConfig {
                /*
                   //Kafka Config Template
                   BootstrapServers = "localhost:9092",
                   SaslMechanism = SaslMechanism.Plain,
                   SecurityProtocol = SecurityProtocol.SaslSsl,
                   SaslUsername = "xxxxxxx",
                   SaslPassword = "xxxx+"

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
                SaslPassword = $"Endpoint=sb://{eventHubNameSpace}.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={sasToken}", //EventHub NameSpace Key1
                
                //Optional Kafka settings below - to maintain message ordering and prevent duplication
                EnableIdempotence = true,
                Acks = Acks.All

            };

            using (var p = new ProducerBuilder<long, string>(config).Build())
            {
                string brokerList = config.BootstrapServers.ToString();

                try
                {
                    Console.WriteLine("Sending " + numberOfEventsSent + " message/s to topic: " + topic + ", broker(s): " + brokerList);

                    string msg;

                    for (int i = 1; i < numberOfEventsSent+1; i++)
                    {
                        if (useJsonAsEventFormat)
                        {
                            //option 1: Delivering message as json
                            Router router = new Router();
                            RouterTelemetry routerTel = new RouterTelemetry();

                            //Generate Random Router telemtry Data
                            router = routerTel.GenerateRouterData();

                            //wrap the telemetry data in json format
                            string JSONresult = JsonConvert.SerializeObject(router);

                            //Dump the json output on local machine for review
                            if (dumpJsonOutput.Equals(true))
                            {
                                DumpJsonOutput(JSONresult);
                            }

                            msg = JSONresult;

                        }
                        else
                        {
                            //option 2: Delivering message as a | string
                            WifiTelemetryEvent tevent = getWifiTeleEvent();
                            msg = string.Format("msgID: {0} | timestamp: {1} | ssid: {2} | signal: {3} | connectedDeviceID: {4} | downStreamKbps: {5} | upStreamKbps: {6} | routerTemp: {7}"
                                , i
                                , DateTime.Now.ToString("yyyy-MM-dd_HH:mm:ss.ff")
                                , tevent.ssid
                                , tevent.signal
                                , tevent.connectedDeviceID
                                , tevent.downStreamKbps
                                , tevent.upStreamKbps
                                , tevent.routerTemp
                                );
                        }
                        
                        //Send the message to the kafka cluster
                        var deliveryReport = await p.ProduceAsync(topic, new Message<long, string> { Key = DateTime.UtcNow.Ticks, Value = msg });

                        //output the sended message in console
                        Console.WriteLine(string.Format("Message {0} sent (value: '{1}') to TopicPartition and its Offset: {2}", i, msg, deliveryReport.TopicPartitionOffset));
                    }

                }
                catch (ProduceException<long, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }

            Console.ReadLine();
            /*
             * https://xyzcoder.github.io/kafka/kafka_csharp/confluent_kafka/kafka_on_windows/2019/02/26/getting-started-with-kafka-and-c.html
             * 
             */
        }


        private static void DumpJsonOutput(string JSONresult)
        {
            /*
             *  Dump the json output on local machine for review
             */

            string path = @".\jsonDumps\" + DateTime.Now.ToString("yyyyMMdd_THHmm");
            string FilePath = path + @"\router" + DateTime.Now.ToString("yyyyMMdd_THHmmss_fffffff") + ".json";

            // If directory does not exist, create it.  
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            if (File.Exists(FilePath))
            {
                File.Delete(FilePath);
                CreateJsonFile(JSONresult, FilePath);
            }
            else if (!File.Exists(FilePath))
            {
                CreateJsonFile(JSONresult, FilePath);
            }
        }

        private static void CreateJsonFile(string JSONresult, string FilePath)
        {
            using (var tw = new StreamWriter(FilePath, true))
            {
                tw.WriteLine(JSONresult.ToString());
                tw.Close();
            }
        }

        public static WifiTelemetryEvent getWifiTeleEvent()
        {   
            /*
                Examlpe of wifi telemetry
             */
            WifiTelemetryEvent evt = new WifiTelemetryEvent();

            evt.ssid = "myWifiName123";
            evt.connectedDeviceID = new Random().Next(1, 3); // Up to 3 connected devices
            evt.signal = new Random().Next(-70, -30); //-70 dBm Weak | -60 dBm to -70 dBm Fair | -50 dBm to -60 dBm Good | > -50 dBm Excellent
            evt.downStreamKbps = new Random().Next(0, 150000); //max 150 mbit
            evt.upStreamKbps = new Random().Next(0, 150000); //max 150 mbit
            evt.routerTemp = new Random().Next(15, 45); 
            return evt;
        }        
    }
}