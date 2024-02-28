using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ErrorRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:ERROR";

        private readonly Action<ErrorRecord> _consumer;

        public ErrorRecordConsumer(Action<ErrorRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ErrorRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
