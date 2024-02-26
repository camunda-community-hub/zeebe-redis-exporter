using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class ProcessInstanceCreationRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:PROCESS_INSTANCE_CREATION";

        private readonly Action<ProcessInstanceCreationRecord> _consumer;

        public ProcessInstanceCreationRecordConsumer(Action<ProcessInstanceCreationRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessInstanceCreationRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
