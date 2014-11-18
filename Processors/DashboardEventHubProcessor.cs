using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.ServiceRuntime;
using Newtonsoft.Json;

namespace Processors
{
    public class DashboardEventHubProcessor : IEventProcessor
    {
        private ReceiveUnit ru;
        DateTime lastCheckpoint;
        bool firstMessage = true;
        QueueClient qclient;
        System.Collections.Generic.Dictionary<string, float> solarCells = null;
        string queueConnectionString;

        public DashboardEventHubProcessor()
        {
            ru = new ReceiveUnit();
            ru.receiverId = System.Environment.MachineName;
            string serviceBusConnectionString = RoleEnvironment.GetConfigurationSettingValue("ServiceBusConnectionString");
            var queueBuilder = new ServiceBusConnectionStringBuilder(serviceBusConnectionString)
            {
                TransportType = TransportType.NetMessaging
            };
            queueConnectionString = queueBuilder.ToString();

            try
            {
                qclient = QueueClient.CreateFromConnectionString(queueConnectionString, "stelemetryqueue");
            }
            catch (Exception e)
            {
                // Nothing
            }
        }

        public async Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            
        }

        public async Task OpenAsync(PartitionContext context)
        {
            Trace.WriteLine("context.Lease.PartitionId = " + context.Lease.PartitionId);
            lastCheckpoint = DateTime.Now;
            solarCells = new Dictionary<string, float>();

            ru.PartitionId = context.Lease.PartitionId;
        }

        public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (EventData message in messages)
            {
                try
                {
                    Trace.WriteLine("Count: " + ru.numberOfItems +" Partition: "+context.Lease.PartitionId);
                    if (firstMessage)
                    {
                        ru.StartTime = DateTime.Now;
                        firstMessage = false;
                        ru.numberOfItems = 0;
                    }
                    int sizeOfChunk = 0;

                    if (RoleEnvironment.IsEmulated)
                        sizeOfChunk = 100;
                    else
                        sizeOfChunk = 10000;

                    if (ru.numberOfItems == sizeOfChunk)   
                    {
                        ru.EndTime = DateTime.Now;
                        firstMessage = true;
                    
                        // Send to SendQueue
                        string output = JsonConvert.SerializeObject(ru);
                        
                        try
                        {
                            var msg = new BrokeredMessage("EventHubs Metadata");
                            msg.TimeToLive = TimeSpan.FromMinutes(5);
                            msg.Properties["recunit"] = output;
                            await qclient.SendAsync(msg);
                        }
                        catch(Exception e)
                        {
                            Trace.WriteLine("Exception sending message to metadata queue: " + e.Message);
                        }
                        
                    }

                    ru.numberOfItems++;

                    if (message.Properties.Count > 0)
                    {
                        float currentEnergyProduction = (float)message.Properties["current"];
                        string key = string.Format("{0}|{1}", message.Properties["panel-id"], message.Properties["field-id"]);
                        if (solarCells.ContainsKey(key))
                        {
                            solarCells[key] += currentEnergyProduction;
                        }
                        else
                        {
                            solarCells.Add(key, currentEnergyProduction);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Trace.WriteLine("Exception in ProcessEventsAsync: " + ex.Message);
                }
            }

        }
    }

    public class ReceiveUnit
    {
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public int numberOfItems { get; set; }
        public string receiverId { get; set; }
        public string PartitionId { get; set; }
    }

    
}
