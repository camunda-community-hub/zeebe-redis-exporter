using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class ProcessEventRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:PROCESS_EVENT";

        private readonly Action<ProcessEventRecord> _consumer;

        public ProcessEventRecordConsumer(Action<ProcessEventRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessEventRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
