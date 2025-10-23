using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class MappingRuleRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:MAPPING_RULE";

        private readonly Action<MappingRuleRecord> _consumer;

        public MappingRuleRecordConsumer(Action<MappingRuleRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out MappingRuleRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
