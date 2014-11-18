using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace Processors
{
    public class StorageEventHubProcessor : IEventProcessor
    {
        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            throw new NotImplementedException();
        }

        public Task OpenAsync(PartitionContext context)
        {
            throw new NotImplementedException();
        }

        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            throw new NotImplementedException();
        }
    }
}
