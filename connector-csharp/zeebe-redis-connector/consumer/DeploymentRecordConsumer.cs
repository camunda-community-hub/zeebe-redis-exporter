using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class DeploymentRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:DEPLOYMENT";

        private readonly Action<DeploymentRecord> _consumer;

        public DeploymentRecordConsumer(Action<DeploymentRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out DeploymentRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
