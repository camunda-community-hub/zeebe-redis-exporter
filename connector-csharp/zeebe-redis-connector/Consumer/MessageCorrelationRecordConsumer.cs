using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class MessageCorrelationRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:MESSAGE_CORRELATION";

        private readonly Action<MessageCorrelationRecord> _consumer;

        public MessageCorrelationRecordConsumer(Action<MessageCorrelationRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out MessageCorrelationRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
