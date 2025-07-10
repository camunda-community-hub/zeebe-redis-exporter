using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class MessageBatchRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:MESSAGE_BATCH";

        private readonly Action<MessageBatchRecord> _consumer;

        public MessageBatchRecordConsumer(Action<MessageBatchRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out MessageBatchRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
