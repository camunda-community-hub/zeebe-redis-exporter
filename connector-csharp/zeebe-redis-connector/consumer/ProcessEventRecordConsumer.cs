using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ProcessEventRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:PROCESS_EVENT";

        private readonly Action<ProcessEventRecord> _consumer;

        public ProcessEventRecordConsumer(Action<ProcessEventRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessEventRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
