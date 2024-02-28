using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class VariableDocumentRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:VARIABLE_DOCUMENT";

        private readonly Action<VariableDocumentRecord> _consumer;

        public VariableDocumentRecordConsumer(Action<VariableDocumentRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out VariableDocumentRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
