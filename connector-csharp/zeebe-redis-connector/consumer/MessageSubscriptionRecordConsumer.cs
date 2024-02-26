using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class MessageSubscriptionRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:MESSAGE_SUBSCRIPTION";

        private readonly Action<MessageSubscriptionRecord> _consumer;

        public MessageSubscriptionRecordConsumer(Action<MessageSubscriptionRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out MessageSubscriptionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
