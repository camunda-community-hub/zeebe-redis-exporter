using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ExpressionRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:EXPRESSION";

        private readonly Action<ExpressionRecord> _consumer;

        public ExpressionRecordConsumer(Action<ExpressionRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ExpressionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}

