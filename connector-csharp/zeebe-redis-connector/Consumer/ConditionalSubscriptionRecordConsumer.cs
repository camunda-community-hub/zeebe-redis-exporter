using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ConditionalSubscriptionRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:CONDITIONAL_SUBSCRIPTION";

        private readonly Action<ConditionalSubscriptionRecord> _consumer;

        public ConditionalSubscriptionRecordConsumer(Action<ConditionalSubscriptionRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ConditionalSubscriptionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}

