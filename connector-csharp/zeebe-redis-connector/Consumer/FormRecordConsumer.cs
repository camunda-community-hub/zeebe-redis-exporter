using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class FormRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:FORM";

        private readonly Action<FormRecord> _consumer;

        public FormRecordConsumer(Action<FormRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out FormRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
