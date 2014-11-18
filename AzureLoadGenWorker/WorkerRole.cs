using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;

using LoadGenerator;

namespace AzureLoadGenWorker
{
    using System.Threading.Tasks;

    public class WorkerRole : RoleEntryPoint
    {
        CancellationTokenSource cts;

        public override void Run()
        {
            cts = new CancellationTokenSource();

            Trace.TraceInformation("AzureLoadGenWorker entry point called");
            
            string sbConnectionString = RoleEnvironment.GetConfigurationSettingValue("ServiceBusConnectionString");
            
            string eventHubName = RoleEnvironment.GetConfigurationSettingValue("EventHubName");
            
            int partitionCount = int.Parse(RoleEnvironment.GetConfigurationSettingValue("PartitionCount"));
            
            int fieldCount = int.Parse(RoleEnvironment.GetConfigurationSettingValue("FieldCount"));
            
            int numberOfCells = int.Parse(RoleEnvironment.GetConfigurationSettingValue("NumberOfCells"));
            
            Frequency frequency = (Frequency)Enum.Parse(typeof(Frequency), RoleEnvironment.GetConfigurationSettingValue("Frequency"));
            
            Generator generator = new Generator(partitionCount, fieldCount, numberOfCells, frequency, sbConnectionString, eventHubName);
            
            List<Task> senders = new List<Task>();
            for (int i = 0; i < 20; i++)
                senders.Add(generator.SendEventsAsync(cts.Token, i.ToString()));

            Task.WhenAll(senders.ToArray()).GetAwaiter().GetResult();
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections 
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            return base.OnStart();
        }

        public override void OnStop()
        {
            base.OnStop();
            cts.Cancel();
        }
    }
}
