using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ProcessInstanceBatchRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:PROCESS_INSTANCE_BATCH";

        private readonly Action<ProcessInstanceBatchRecord> _consumer;

        public ProcessInstanceBatchRecordConsumer(Action<ProcessInstanceBatchRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessInstanceBatchRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
