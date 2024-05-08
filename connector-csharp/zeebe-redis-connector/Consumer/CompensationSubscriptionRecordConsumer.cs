using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class CompensationSubscriptionRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:COMPENSATION_SUBSCRIPTION";

        private readonly Action<CompensationSubscriptionRecord> _consumer;

        public CompensationSubscriptionRecordConsumer(Action<CompensationSubscriptionRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out CompensationSubscriptionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
