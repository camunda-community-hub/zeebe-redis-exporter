using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks;

namespace Io.Zeebe.Redis.Connect.Csharp.Hosting
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddZeebeRedis(this IServiceCollection services, IConfiguration namedConfigurationSection)
        {
            if (namedConfigurationSection is null)
            {
                throw new ArgumentNullException(nameof(namedConfigurationSection));
            }

            services
                .AddHostedService<ZeebeRedisHostedService>()
                .AddSingleton<ZeebeRedis>()
                .AddOptions<ZeebeRedisOptions>()
                .Bind(namedConfigurationSection)
                .Validate(ValidateZeebeRedisOptions);

            return services;
        }

        public static IServiceCollection AddZeebeRedis(this IServiceCollection services, Action<ZeebeRedisOptions> configureOptions)
        {
            if (configureOptions is null)
            {
                throw new ArgumentNullException(nameof(configureOptions));
            }


            services
                .AddHostedService<ZeebeRedisHostedService>()
                .AddSingleton<ZeebeRedis>()
                .AddOptions<ZeebeRedisOptions>()
                .Configure(configureOptions)
                .Validate(ValidateZeebeRedisOptions);

            return services;
        }

        public static IServiceCollection AddZeebeRedis(this IServiceCollection services)
        {
            services
                .AddHostedService<ZeebeRedisHostedService>()
                .AddSingleton<ZeebeRedis>()
                .AddOptions<ZeebeRedisOptions>()
                .Configure((options) => {})
                .Validate(ValidateZeebeRedisOptions);

            return services;
        }

        private static bool ValidateZeebeRedisOptions(ZeebeRedisOptions options)
        {
            return options.Validate();
        }
    }
}
