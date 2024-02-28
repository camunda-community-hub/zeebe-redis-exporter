using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class VariableRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:VARIABLE";

        private readonly Action<VariableRecord> _consumer;

        public VariableRecordConsumer(Action<VariableRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out VariableRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
