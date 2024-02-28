using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class IncidentRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:INCIDENT";

        private readonly Action<IncidentRecord> _consumer;

        public IncidentRecordConsumer(Action<IncidentRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out IncidentRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
