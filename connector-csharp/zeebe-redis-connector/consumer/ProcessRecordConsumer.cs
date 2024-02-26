using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class ProcessRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:PROCESS";

        private readonly Action<ProcessRecord> _consumer;

        public ProcessRecordConsumer(Action<ProcessRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
