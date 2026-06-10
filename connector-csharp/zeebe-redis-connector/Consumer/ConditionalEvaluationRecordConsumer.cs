using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ConditionalEvaluationRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:CONDITIONAL_EVALUATION";

        private readonly Action<ConditionalEvaluationRecord> _consumer;

        public ConditionalEvaluationRecordConsumer(Action<ConditionalEvaluationRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ConditionalEvaluationRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}

