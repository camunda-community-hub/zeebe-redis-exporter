using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class MultiInstanceRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:MULTI_INSTANCE";

        private readonly Action<MultiInstanceRecord> _consumer;

        public MultiInstanceRecordConsumer(Action<MultiInstanceRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out MultiInstanceRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
