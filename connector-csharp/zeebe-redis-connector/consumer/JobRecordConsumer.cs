using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class JobRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:JOB";

        private readonly Action<JobRecord> _consumer;

        public JobRecordConsumer(Action<JobRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out JobRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
