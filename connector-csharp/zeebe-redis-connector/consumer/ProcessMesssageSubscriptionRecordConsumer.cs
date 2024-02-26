using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class ProcessMessageSubscriptionRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:PROCESS_MESSAGE_SUBSCRIPTION";

        private readonly Action<ProcessMessageSubscriptionRecord> _consumer;

        public ProcessMessageSubscriptionRecordConsumer(Action<ProcessMessageSubscriptionRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessMessageSubscriptionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
