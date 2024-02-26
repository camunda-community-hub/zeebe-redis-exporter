using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class TimerRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:TIMER";

        private readonly Action<TimerRecord> _consumer;

        public TimerRecordConsumer(Action<TimerRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out TimerRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
