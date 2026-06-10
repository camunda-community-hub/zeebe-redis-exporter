using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class GlobalListenerRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:GLOBAL_LISTENER";

        private readonly Action<GlobalListenerRecord> _consumer;

        public GlobalListenerRecordConsumer(Action<GlobalListenerRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out GlobalListenerRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}

