using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class DeploymentDistributionRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:DEPLOYMENT_DISTRIBUTION";

        private readonly Action<DeploymentDistributionRecord> _consumer;

        public DeploymentDistributionRecordConsumer(Action<DeploymentDistributionRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out DeploymentDistributionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
