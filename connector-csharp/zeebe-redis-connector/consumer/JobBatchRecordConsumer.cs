using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class JobBatchRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:JOB_BATCH";

        private readonly Action<JobBatchRecord> _consumer;

        public JobBatchRecordConsumer(Action<JobBatchRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out JobBatchRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
