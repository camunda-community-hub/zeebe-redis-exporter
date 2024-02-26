using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class MessageRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:MESSAGE";

        private readonly Action<MessageRecord> _consumer;

        public MessageRecordConsumer(Action<MessageRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out MessageRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
