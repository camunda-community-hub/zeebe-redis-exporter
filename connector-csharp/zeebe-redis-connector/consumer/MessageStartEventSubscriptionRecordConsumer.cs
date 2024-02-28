using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class MessageStartEventSubscriptionRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:MESSAGE_START_EVENT_SUBSCRIPTION";

        private readonly Action<MessageStartEventSubscriptionRecord> _consumer;

        public MessageStartEventSubscriptionRecordConsumer(Action<MessageStartEventSubscriptionRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out MessageStartEventSubscriptionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
