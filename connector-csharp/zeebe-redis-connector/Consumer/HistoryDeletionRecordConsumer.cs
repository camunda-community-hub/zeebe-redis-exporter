using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class HistoryDeletionRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:HISTORY_DELETION";

        private readonly Action<HistoryDeletionRecord> _consumer;

        public HistoryDeletionRecordConsumer(Action<HistoryDeletionRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out HistoryDeletionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}

