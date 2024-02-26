using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class JobBatchRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:JOB_BATCH";

        private readonly Action<JobBatchRecord> _consumer;

        public JobBatchRecordConsumer(Action<JobBatchRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out JobBatchRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
