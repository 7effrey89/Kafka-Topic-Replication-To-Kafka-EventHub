# Kafka To EventHub Demo
Demo includes a Kafka Producer that write events to a topic in a Kafka Cluster (using EventHub with Kafka Endpoints instead), and then using an Azure Function to consume from the topic and send it to an EventHub. Additional stream jobs are designed to consume from the event hub cluster

# Solution Architecture
![Architecture](/Screenshots/Architecture.png?raw=true "Architecture")

# Premade Arm Template
The template will deploy the following:
- EventHub Namespace and EventHub to imitate a Kafka Cluster with a topic called 'topica'. Will be refered to as **KafkaSource** in this demo.
- EventHub Namespace and EventHub that is acting as a replica of the Kafka Cluster. Will be refered to as **EventHubClone** in this demo.
- Stream Analytic Job to sink and flatten the json output received from EventHubClone. Will be refered to as **FlatDumpStreamJob**
- Stream Analytic Job to stream and transform the recieved json output to PowerBi in realtime. Will be refered to as **AggregationStreamJob**

Template can be downloaded here: https://github.com/jeffreylai_microsoft/KafkaToEventHubDemo/blob/main/Arm%20Template/templateWithoutFunction.json

# Excersise
The main exercise in this lab will be to produce an Azure Function to consume and send a message between Kafka and EventHub.
Optional excersises have been added to inspire how to consume streaming data from EventHub.

The lab Guide can be downloaded here: https://github.com/jeffreylai_microsoft/KafkaToEventHubDemo/blob/main/Streaming%20Demo%20Lab.pdf

# Code Samples below used in the lab. More can be found in the repo.

# Azure Function Code
```#r "Microsoft.Azure.WebJobs.Extensions.Kafka"

using System;
using System.Text;
using Microsoft.Azure.WebJobs.Extensions.Kafka;

public static void Run(KafkaEventData<string> eventData, ILogger log, out string outputEventHubMessage)
{   
    //remember to add "out string outputEventHubMessage" as parameter for Run in the line above
    
    //Logs the message received from kafka
    log.LogInformation($"C# Kafka trigger function processed a message: {eventData.Value}");
    //Sends the message to event hub.
    outputEventHubMessage = eventData.Value;

}
```

# Stream Analytics Job: Query for FlatDumpStreamJob 
```WITH cte as (
    SELECT
         event.customerID
        , event.ssid
        , event.routerTemp
        , event.channel
        , event.timestamp
        , connectedDevice.ArrayValue.*
        /*
        ,connectedDevice.ArrayValue.deviceID
        ,connectedDevice.ArrayValue.SignalStrenth
        ,connectedDevice.ArrayValue.downStreamKbps
        ,connectedDevice.ArrayValue.upStreamKbps
        ,connectedDevice.ArrayValue.webConnections
        */
        , CONCAT(DATEPART(year,event.timestamp) , DATEPART(mm,event.timestamp), DATEPART(dd,event.timestamp)) as PartitionKey
    --INTO
    --    FlatDumpMinDataLake
    FROM
        [topica-clone] as event
    CROSS APPLY GetArrayElements(event.connectedDevices) AS connectedDevice
)
SELECT 
    main.customerID
    ,main.ssid
    ,main.routerTemp
    ,main.channel
    ,main.timestamp
    ,main.deviceID
    ,main.SignalStrenth
    ,main.downStreamKbps
    ,main.upStreamKbps
    ,main.PartitionKey
    ,webConnection.ArrayValue.*
INTO
    FlatDumpMinDataLake
FROM cte as main
CROSS APPLY GetArrayElements(main.webConnections) AS webConnection
```

# Stream Analytics Job: Query for AggregationStreamJob 
```WITH CTE AS (
    SELECT
        System.Timestamp() AS WindowEnd
        , event.customerID
        , event.ssid
        , connectedDevice.ArrayValue.deviceID
        , round(avg(connectedDevice.ArrayValue.downStreamKbps)/1024,2) AS AvgDeviceDownStreamMbps
        , round(avg(connectedDevice.ArrayValue.upStreamKbps)/1024,2) AS AvgDeviceupStreamMbps
        , round(min(connectedDevice.ArrayValue.signalStrength),2) AS MinSignalStrength_dBm
        , round(max(connectedDevice.ArrayValue.signalStrength),2) AS MaxSignalStrength_dBm
        , round(avg(connectedDevice.ArrayValue.signalStrength),2) AS AvgSignalStrength_dBm
        , connectedDevice.ArrayValue.webConnections

    FROM
        [minEventHub] AS event TIMESTAMP BY event.timestamp
    CROSS APPLY GetArrayElements(event.connectedDevices) AS connectedDevice
    GROUP BY 
        event.customerID
        , event.ssid
        , connectedDevice.ArrayValue.deviceID
        , connectedDevice.ArrayValue.webConnections
        , TumblingWindow(second,10)
)
SELECT 
    main.WindowEnd
    , main.customerID
    , main.ssid
    , main.deviceID
    , min(main.AvgDeviceDownStreamMbps) AS AvgDeviceDownStreamMbps
    , min(main.AvgDeviceupStreamMbps) AS AvgDeviceupStreamMbps
    , min(main.MinSignalStrength_dBm) AS MinSignalStrength_dBm
    , min(main.MaxSignalStrength_dBm) AS MaxSignalStrength_dBm
    , min(main.AvgSignalStrength_dBm) AS AvgSignalStrength_dBm
    , webConnection.ArrayValue.source
    , webConnection.ArrayValue.destination
    , round(avg(webConnection.ArrayValue.latency_ms),2) AS AvgLatency_ms
    , round(avg(webConnection.ArrayValue.packetLoss_percentage),2) AS AvgPacketLoss_percentage
    , round(avg(webConnection.ArrayValue.jitter_ms),2) AS AvgJitter_ms
INTO
    MyPowerBiOutput
FROM CTE AS main
CROSS APPLY GetArrayElements(main.webConnections) AS webConnection
GROUP BY 
     main.WindowEnd
    , main.customerID
    , main.ssid
    , main.deviceID
    , webConnection.ArrayValue.source
    , webConnection.ArrayValue.destination
    , TumblingWindow(second,10)
```
