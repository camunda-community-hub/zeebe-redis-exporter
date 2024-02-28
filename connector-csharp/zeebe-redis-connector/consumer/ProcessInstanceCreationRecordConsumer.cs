using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ProcessInstanceCreationRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:PROCESS_INSTANCE_CREATION";

        private readonly Action<ProcessInstanceCreationRecord> _consumer;

        public ProcessInstanceCreationRecordConsumer(Action<ProcessInstanceCreationRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessInstanceCreationRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
