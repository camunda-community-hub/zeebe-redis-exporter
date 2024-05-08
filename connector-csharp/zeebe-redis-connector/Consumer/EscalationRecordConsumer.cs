using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class EscalationRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:ESCALATION";

        private readonly Action<EscalationRecord> _consumer;

        public EscalationRecordConsumer(Action<EscalationRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out EscalationRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
