using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ClockRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:CLOCK";

        private readonly Action<ClockRecord> _consumer;

        public ClockRecordConsumer(Action<ClockRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ClockRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
