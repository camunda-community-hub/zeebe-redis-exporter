using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class VariableDocumentRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:VARIABLE_DOCUMENT";

        private readonly Action<VariableDocumentRecord> _consumer;

        public VariableDocumentRecordConsumer(Action<VariableDocumentRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out VariableDocumentRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
