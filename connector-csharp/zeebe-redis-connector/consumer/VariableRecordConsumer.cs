using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class VariableRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:VARIABLE";

        private readonly Action<VariableRecord> _consumer;

        public VariableRecordConsumer(Action<VariableRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out VariableRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
