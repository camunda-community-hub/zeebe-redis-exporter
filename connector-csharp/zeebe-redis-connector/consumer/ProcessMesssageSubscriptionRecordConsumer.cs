using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ProcessMessageSubscriptionRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:PROCESS_MESSAGE_SUBSCRIPTION";

        private readonly Action<ProcessMessageSubscriptionRecord> _consumer;

        public ProcessMessageSubscriptionRecordConsumer(Action<ProcessMessageSubscriptionRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessMessageSubscriptionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
