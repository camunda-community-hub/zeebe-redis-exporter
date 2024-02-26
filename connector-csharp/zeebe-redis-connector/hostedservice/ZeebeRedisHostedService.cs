using Io.Zeebe.Redis.Connect.Csharp;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Io.Zeebe.Redis.Connect.Csharp.Hosting
{
    public class ZeebeRedisHostedService : IHostedService, IDisposable
    {
        private CancellationTokenSource? cancellationTokenSource;
        private readonly ILogger<ZeebeRedisHostedService> _logger;
        private readonly ZeebeRedis _zeebeRedis;

        private Task? _executeTask;

        public ZeebeRedisHostedService(ZeebeRedis zeebeRedis, ILoggerFactory loggerFactory) 
        {
            this._logger = loggerFactory?.CreateLogger<ZeebeRedisHostedService>() ?? throw new ArgumentNullException(nameof(loggerFactory));
            this._zeebeRedis = zeebeRedis ?? throw new ArgumentNullException(nameof(zeebeRedis));
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            this.cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            _executeTask = _zeebeRedis.StartConsumeEvents(this.cancellationTokenSource);
            // If the task is completed then return it, this will bubble cancellation and failure to the caller
            if (_executeTask.IsCompleted)
            {
                return _executeTask;
            }

            // Otherwise it's running
            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            // Stop called without start
            if (_executeTask == null)
            {
                return;
            }

            try
            {
                // Signal cancellation to the executing method
                cancellationTokenSource?.Cancel();
            }
            finally
            {
                // Wait until the task completes or the stop token triggers
                await Task.WhenAny(_executeTask, Task.Delay(Timeout.Infinite, cancellationToken)).ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            _zeebeRedis?.Dispose();
        }
    }
}
