using Io.Zeebe.Exporter.Proto;
using System;

namespace zeebe_redis_connector.consumer
{
    public class DeploymentDistributionRecordConsumer : IRecordConsumer
    {
        public static String STREAM = "zeebe:DEPLOYMENT_DISTRIBUTION";

        private readonly Action<DeploymentDistributionRecord> _consumer;

        public DeploymentDistributionRecordConsumer(Action<DeploymentDistributionRecord> action) {
            this._consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out DeploymentDistributionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
