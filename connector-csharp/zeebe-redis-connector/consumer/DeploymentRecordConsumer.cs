using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class DeploymentRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:DEPLOYMENT";

        private readonly Action<DeploymentRecord> _consumer;

        public DeploymentRecordConsumer(Action<DeploymentRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out DeploymentRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
