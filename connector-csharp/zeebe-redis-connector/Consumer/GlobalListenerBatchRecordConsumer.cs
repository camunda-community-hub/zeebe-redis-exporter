using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class GlobalListenerBatchRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:GLOBAL_LISTENER_BATCH";

        private readonly Action<GlobalListenerBatchRecord> _consumer;

        public GlobalListenerBatchRecordConsumer(Action<GlobalListenerBatchRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out GlobalListenerBatchRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}

