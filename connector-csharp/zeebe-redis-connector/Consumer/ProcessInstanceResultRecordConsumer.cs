using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ProcessInstanceResultRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:PROCESS_INSTANCE_RESULT";

        private readonly Action<ProcessInstanceResultRecord> _consumer;

        public ProcessInstanceResultRecordConsumer(Action<ProcessInstanceResultRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessInstanceResultRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
