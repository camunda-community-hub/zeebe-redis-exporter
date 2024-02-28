using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ProcessRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:PROCESS";

        private readonly Action<ProcessRecord> _consumer;

        public ProcessRecordConsumer(Action<ProcessRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
