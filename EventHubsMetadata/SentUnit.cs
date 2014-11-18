using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubsMetadata
{

    public class SentUnit
    {
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public int numberOfItems { get; set; }

        public string senderId { get; set; }
    }
}
