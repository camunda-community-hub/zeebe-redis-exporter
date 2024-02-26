using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class JobRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:JOB";

        private readonly Action<JobRecord> _consumer;

        public JobRecordConsumer(Action<JobRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out JobRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
