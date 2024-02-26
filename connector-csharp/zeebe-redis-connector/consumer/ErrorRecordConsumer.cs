using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class ErrorRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:ERROR";

        private readonly Action<ErrorRecord> _consumer;

        public ErrorRecordConsumer(Action<ErrorRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ErrorRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
