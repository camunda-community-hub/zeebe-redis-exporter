using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ProcessInstanceRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:PROCESS_INSTANCE";

        private readonly Action<ProcessInstanceRecord> _consumer;

        public ProcessInstanceRecordConsumer(Action<ProcessInstanceRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessInstanceRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
