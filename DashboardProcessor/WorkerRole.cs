
namespace DashboardProcessor
{
    using System;
    using System.Collections;
    using System.Diagnostics;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.ServiceRuntime;
    using Processors;

    public class WorkerRole : RoleEntryPoint
    {
        bool ephListening = false;
        CancellationTokenSource cancellationSource;
        QueueClient machineControlQueueClient;
        QueueClient controlReplyQueueClient;
        EventProcessorHost eventProcessorHost;
        QueueClient telemetryQueueClient;

        public WorkerRole()
        {
            cancellationSource = new CancellationTokenSource();
        }

        public override void Run()
        {
            RunAsync().GetAwaiter().GetResult();
        }

        public async Task RunAsync()
        {
            // This is a sample worker implementation. Replace with your logic.
            Trace.TraceInformation("DashboardProcessor entry point called");

            // Create the control channel queue
            var factory = MessagingFactory.CreateFromConnectionString(RoleEnvironment.GetConfigurationSettingValue("ServiceBusConnectionString"));
            var namespaceManager = NamespaceManager.CreateFromConnectionString(RoleEnvironment.GetConfigurationSettingValue("ServiceBusConnectionString"));

            try
            {
                await namespaceManager.CreateQueueAsync(System.Environment.MachineName);
            }
            catch (Exception e)
            {
                Trace.TraceInformation("Already Exists");
            }

            machineControlQueueClient = factory.CreateQueueClient(System.Environment.MachineName, ReceiveMode.ReceiveAndDelete);
            telemetryQueueClient = factory.CreateQueueClient("stelemetryqueue");
            controlReplyQueueClient = factory.CreateQueueClient("controlQueue");

            // timeout value for Control Channel Queue receive
            int secs = 10;

            while (!cancellationSource.IsCancellationRequested)
            {
                try
                {
                    try
                    {
                        if (ephListening)
                        {
                            await SendControlMessage("STARTED");
                        }
                        else
                        {
                            await SendControlMessage("STOPPED");
                        }
                    }
                    catch (Exception e)
                    {
                        Trace.TraceInformation("Caught on send");
                    }
                    
                    var rcvmsg = await machineControlQueueClient.ReceiveAsync(TimeSpan.FromSeconds(secs));
                    if (rcvmsg != null)
                    {
                        await ProcessMessage(rcvmsg);
                    }
                }
                catch (MessagingException me)
                {
                    if (!me.IsTransient)
                    {
                        throw;
                    }
                }
            }

            await StopEventProcessorHost();
        }

        async Task SendControlMessage(string controlMessage)
        {
            var msg = new BrokeredMessage("EventHubs Metadata");
            msg.TimeToLive = TimeSpan.FromSeconds(20);
            msg.Properties["controlrec"] = System.Environment.MachineName;
            msg.Properties["controlmsg"] = controlMessage;
            await controlReplyQueueClient.SendAsync(msg);
            Trace.TraceInformation("Sent " + msg.Properties["controlmsg"] + " Message");
        }


        async Task ProcessMessage(BrokeredMessage msg)
        {
            if (msg != null)
            {
                if (msg.Properties.ContainsKey("command"))
                {
                    string command = (string)msg.Properties["command"];
                    switch (command)
                    {
                        case "ToggleEPH":
                            if (!ephListening)
                            {
                                await StartEventProcessorHost();
                                await SendControlMessage("STARTED");
                                ephListening = true;
                            }
                            else
                            {
                                await StopEventProcessorHost();
                                await SendControlMessage("STOPPED");
                                ephListening = false;
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        async Task StartEventProcessorHost()
        {
            string eventHubConnectionString = CloudConfigurationManager.GetSetting("ServiceBusConnectionString");
            string blobConnectionString = CloudConfigurationManager.GetSetting("AzureStorageConnectionString");
            // Required for checkpoint/state
            string EventHubName = CloudConfigurationManager.GetSetting("EventHubName");
            eventProcessorHost = new EventProcessorHost(System.Environment.MachineName, EventHubName,
                EventHubConsumerGroup.DefaultGroupName, eventHubConnectionString, blobConnectionString);
            EventProcessorOptions options = new EventProcessorOptions() {MaxBatchSize = 8192};

            await eventProcessorHost.RegisterEventProcessorAsync<DashboardEventHubProcessor>();
        }

        async Task StopEventProcessorHost()
        {
            await eventProcessorHost.UnregisterEventProcessorAsync();
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections 
            ServicePointManager.DefaultConnectionLimit = 12;

            return base.OnStart();
        }

        public override void OnStop()
        {
            base.OnStop();
            cancellationSource.Cancel();
        }
    }
}