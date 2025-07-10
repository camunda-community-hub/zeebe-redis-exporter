using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ProcessInstanceMigrationRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:PROCESS_INSTANCE_MIGRATION";

        private readonly Action<ProcessInstanceMigrationRecord> _consumer;

        public ProcessInstanceMigrationRecordConsumer(Action<ProcessInstanceMigrationRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ProcessInstanceMigrationRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
