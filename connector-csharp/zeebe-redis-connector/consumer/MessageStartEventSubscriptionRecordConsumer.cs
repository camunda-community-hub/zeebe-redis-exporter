using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class MessageStartEventSubscriptionRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:MESSAGE_START_EVENT_SUBSCRIPTION";

        private readonly Action<MessageStartEventSubscriptionRecord> _consumer;

        public MessageStartEventSubscriptionRecordConsumer(Action<MessageStartEventSubscriptionRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out MessageStartEventSubscriptionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
