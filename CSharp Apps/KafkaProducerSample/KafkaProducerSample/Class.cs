using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Kafka_Producer.Program;

namespace MyTelemetryClasses
{
    internal class Class
    {
    }
    public class WifiTelemetryEvent
    {
        public string ssid;
        public int connectedDeviceID;
        public double signal;
        public double downStreamKbps;
        public double upStreamKbps;
        public double routerTemp;
    }

    public class Router
    {
        public DateTime timestamp;
        public string customerID;
        public string ssid;
        public int channel;
        public double routerTemp;
        public ConnectedDevice[] connectedDevices;


    }
    public class ConnectedDevice
    {
        public string deviceID;
        public double signalStrength;
        public double downStreamKbps;
        public double upStreamKbps;
        public WebConnections[] webConnections;

    }
    public class Customer
    {
        public string CustomerID;
        public string WifiSSID;
    }
    public class CustomerData
    {
        public Customer GetCustomer()
        {
            Customer cus = new Customer();

            int rand = new Random().Next(0, 100);

            if (rand < 80) //80% of the time the router is set to channel 33
            {
                cus.CustomerID = "JeffreyLai";
                cus.WifiSSID = "JeffreyLai_Wifi";
            }
            else
            {
                cus.CustomerID = "LaurenceBen";
                cus.WifiSSID = "LaurenceBen_Wifi";
            }
            return cus;
        }
    }
    public class WebConnections
    {
        public string source;
        public string destination;
        public double latency_ms; // latency is simply the amount of time it takes for data to travel from one defined location to another
        public int packetLoss_percentage; //packet loss refers to the number of packets transmitted from one destination to another that fail to transmit
        public double jitter_ms;
    }
    public class RouterTelemetry
    {

        public Router GenerateRouterData()
        {
            Router router = new Router();
            List<ConnectedDevice> connDevices = new List<ConnectedDevice>();

            Customer cus = new Customer();
            CustomerData cusData = new CustomerData();
            cus = cusData.GetCustomer();

            router.timestamp = DateTime.UtcNow;
            router.customerID = cus.CustomerID;
            router.ssid = cus.WifiSSID;
            router.channel = getRouterChannel();
            router.routerTemp = new Random().Next(15, 45);

            int connectedDevices = new Random().Next(1, 4); //up to 3 devices supported in this demo



            for (int i = 1; i < connectedDevices + 1; i++)
            {
                ConnectedDevice connDevice = new ConnectedDevice();

                connDevice.deviceID = i.ToString();
                connDevice.signalStrength = new Random().Next(-70, -30); //-70 dBm Weak | -60 dBm to -70 dBm Fair | -50 dBm to -60 dBm Good | > -50 dBm Excellent
                connDevice.downStreamKbps = new Random().Next(0, 150000); //max 150 mbit
                connDevice.upStreamKbps = new Random().Next(0, 150000); //max 150 mbit

                List <WebConnections> webConns = new List<WebConnections>();

                WebConnections webConn = new WebConnections();
                WebConnectionsData webConnData = new WebConnectionsData();

                webConn = webConnData.GenerateWebConnection(i);

                webConns.Add(webConn);
                connDevice.webConnections= webConns.ToArray(); 

                connDevices.Add(connDevice);
            }
            router.connectedDevices = connDevices.ToArray();
            return router;
        }

        public class WebConnectionsData
        {
            public WebConnections GenerateWebConnection(int deviceNumber)
            {
                WebConnections webConn = new WebConnections();

                //Random scenarios
                if (deviceNumber==1)
                {
                    webConn.source = "IP of Desktop PC";
                    webConn.destination = "IP of A Popular Gaming Service";
                    webConn.latency_ms = new Random().Next(8, 200);
                    webConn.jitter_ms = new Random().Next(2, 5);
                    webConn.packetLoss_percentage = new Random().Next(0, 5);
                    return webConn;
                }
                if (deviceNumber == 2)
                {
                    Random r = new Random();

                    string[] words = { "IP of A National NewsPaper Website", "IP of A Baby Cloths Webshop", "IP of A Bing Search on X Topic" };

                    webConn.source = "IP of Smart Phone";
                    webConn.destination = words[r.Next(0, words.Length)];
                    webConn.latency_ms = new Random().Next(8, 115);
                    webConn.jitter_ms = new Random().Next(2, 3);
                    webConn.packetLoss_percentage = new Random().Next(0, 0);
                    return webConn;
                }
                if (deviceNumber == 3)
                {
                    webConn.source = "IP of Tablet";
                    webConn.destination = "IP of A National Streaming Service";
                    webConn.latency_ms = new Random().Next(8, 300);
                    webConn.jitter_ms = new Random().Next(2, 5);
                    webConn.packetLoss_percentage = new Random().Next(0, 10);
                    return webConn;
                }

                return webConn;
            }
        }
        private int getRouterChannel()
        {
            int rand = new Random().Next(0, 100);

            if (rand < 80) //80% of the time the router is set to channel 33
            {
                return 33;
            }
            return 44;
        }
    }
}
