using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class MessageSubscriptionRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:MESSAGE_SUBSCRIPTION";

        private readonly Action<MessageSubscriptionRecord> _consumer;

        public MessageSubscriptionRecordConsumer(Action<MessageSubscriptionRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out MessageSubscriptionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
