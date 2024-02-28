using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class TimerRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:TIMER";

        private readonly Action<TimerRecord> _consumer;

        public TimerRecordConsumer(Action<TimerRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out TimerRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
