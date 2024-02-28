using Io.Zeebe.Exporter.Proto;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public interface IRecordConsumer
    {
        void Consume(Record record);
    }
}
