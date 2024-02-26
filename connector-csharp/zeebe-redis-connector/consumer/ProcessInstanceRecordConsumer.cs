using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class ProcessInstanceRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:PROCESS_INSTANCE";

        private readonly Action<ProcessInstanceRecord> _consumer;

        public ProcessInstanceRecordConsumer(Action<ProcessInstanceRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessInstanceRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
