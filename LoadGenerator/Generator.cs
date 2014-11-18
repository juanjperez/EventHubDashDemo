using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Threading;
using System.Diagnostics;
using System.Timers;
using Newtonsoft.Json;
using EventHubsMetadata;
using Microsoft.WindowsAzure.ServiceRuntime;

namespace LoadGenerator
{
    public enum Frequency
    {
        Second = 0,
        TenSecond = 1,
        Minute = 2,
        FifteenMinute = 3
    }

    public class Generator
    {
        int numberOfCells;
        Frequency frequency;
        static SolarCell[] Cells = null;
        static string ServiceBusConnectionString;
        static string queueConnectionString;
        static string EventHubName;
        static QueueClient qclient;
        System.Timers.Timer timer;

        public Generator(int partitionCount, int fieldCount, int numberOfCells, Frequency frequency, string serviceBusConnectionString, string eventHubName)
        {
            Trace.TraceInformation("Creating Generator");
            this.numberOfCells = numberOfCells;
            this.frequency = frequency;
            var builder = new ServiceBusConnectionStringBuilder(serviceBusConnectionString);
            builder.TransportType = TransportType.Amqp;
            ServiceBusConnectionString = builder.ToString();

            var queueBuilder = new ServiceBusConnectionStringBuilder(serviceBusConnectionString);
            queueBuilder.TransportType = TransportType.NetMessaging;
            queueConnectionString = queueBuilder.ToString();

            qclient = QueueClient.CreateFromConnectionString(queueConnectionString, "stelemetryqueue");

            EventHubName = eventHubName;
            Cells = Array.CreateInstance(typeof(SolarCell), numberOfCells) as SolarCell[];
            for (int i = 0; i < numberOfCells; i++)
            {
                Cells[i] = new SolarCell(partitionCount, fieldCount);
            }

        }

        public async Task SendEventsAsync(CancellationToken cts, string senderSuffix)
        {
            var mf = MessagingFactory.CreateFromConnectionString(ServiceBusConnectionString);
            var eventHubClient = mf.CreateEventHubClient(EventHubName);
             
            while (!cts.IsCancellationRequested)
            {
                Trace.TraceInformation("Sending");

                try
                {
                    SentUnit su = new SentUnit();
                    su.senderId = System.Environment.MachineName+"-"+senderSuffix;
                    su.StartTime = DateTime.Now;
                    List<Task> sends = new List<Task>();
                    foreach (SolarCell cell in Cells)
                    {

                        Task sendTask = eventHubClient.SendAsync(cell.MakeMessage());
                        sends.Add(sendTask);
                        su.numberOfItems++;
                        if (su.numberOfItems % 20 == 0)
                        {
                            await Task.WhenAll(sends.ToArray());
                            sends.Clear();
                        }
                    }

                    if (sends.Count > 0)
                    {
                        //Trace.TraceInformation("Sending events up to {0}", su.numberOfItems);
                        await Task.WhenAll(sends.ToArray());
                        sends.Clear();
                    }

                    su.EndTime = DateTime.Now;
                    string output = JsonConvert.SerializeObject(su);

                    BrokeredMessage message = new BrokeredMessage("EventHubs Metadata");
                    message.TimeToLive = TimeSpan.FromMinutes(5);
                    message.Properties["sendunit"] = output;
                    await qclient.SendAsync(message);
                }
                catch (Exception ex)
                {
                    Trace.WriteLine(ex.ToString());
                }

                Trace.TraceInformation("Timer Send Complete");
            }
        }
    }
}
