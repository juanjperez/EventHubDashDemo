using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace LoadGenerator
{
    using Microsoft.ServiceBus.Messaging;
    using Newtonsoft.Json;
    public class SolarReading
    {
        public SolarReading(long fieldId, long gridId, long panelId, Guid deviceId, float current)
        {
            FieldId = fieldId;
            GridId = gridId;
            PanelId = panelId;
            DeviceId = deviceId;
            Current = current;
        }
        public long FieldId { get; private set; }
        public long GridId { get; private set; }
        public long PanelId { get; private set; }
        public Guid DeviceId { get; private set; }
        public float Current { get; private set; }
    }

    internal class SolarCell
    {
        public long FieldId { get; private set; }
        public long GridId { get; private set; }
        public long PanelId { get; private set; }
        public Guid DeviceId { get; private set; }

        Random generator = new Random();
        private const float maxKWPerCell = 0.1F;
        private float averageCurrent = 0F;

        public SolarCell(int partitionCount, int fieldCount)
        {
            PanelId = generator.Next(0, partitionCount);
            DeviceId = Guid.NewGuid();
            GridId = 0;
            FieldId = generator.Next(0, fieldCount);
        }

        float GetCurrent()
        {

            double per = generator.NextDouble();
            float kw = 0.0F;
            // 90% of the cells produce above 75%
            if (per < 0.9)
            {
                kw = (float)((generator.NextDouble() * 0.25) + 0.75) * maxKWPerCell;
            }
            // 5% of the cells produce below 75%
            else if (per > 0.95)
            {
                kw = (float)(0.75 - (generator.NextDouble() * 0.75)) * maxKWPerCell;
            }
            else
            {
                kw = (float)generator.NextDouble();
            }
            if (averageCurrent == 0F)
            {
                averageCurrent = kw;
            }
            else
            {
                averageCurrent = (averageCurrent * 4 + kw) / 5;
            }
            return averageCurrent;
        }

        public EventData MakeMessage()
        {
            SolarReading reading = new SolarReading(FieldId, GridId, PanelId, DeviceId, GetCurrent());
            var serializedString = JsonConvert.SerializeObject(reading);
            EventData msg = new EventData(Encoding.UTF8.GetBytes(serializedString));

            // this ensures all messages to the same panel hit the same partition!!!!
            return msg;
        }
    }
}
