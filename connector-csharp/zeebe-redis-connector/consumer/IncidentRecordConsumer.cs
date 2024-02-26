using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class IncidentRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:INCIDENT";

        private readonly Action<IncidentRecord> _consumer;

        public IncidentRecordConsumer(Action<IncidentRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out IncidentRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
