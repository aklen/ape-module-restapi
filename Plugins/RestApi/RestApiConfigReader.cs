using Ape.Core.Config;
using Ape.Core.Config.Models;

namespace Ape.Module.RestApi.Plugin.RestApi;

internal static class RestApiConfigReader
{
    public static IConfigNode? GetPluginSection(IServiceProvider services)
    {
        var moduleAware = services.GetService(typeof(IModuleTable)) as IModuleTable ?? new ModuleTable();
        var hostCfg = services.GetService(typeof(IStartupConfig)) as IStartupConfig;
        var cfgMgr = services.GetService(typeof(IConfigManager)) as IConfigManager;
        var root = hostCfg?.Root ?? cfgMgr?.StartupRoot;
        var mod = moduleAware.GetModuleSection(root, RestApiModuleIds.ModuleId);
        if (mod != null &&
            mod.TryGetChildObject("plugins", out var plugins) &&
            plugins.TryGetChildObject(RestApiModuleIds.PluginKey, out var fromPlugin))
        {
            return fromPlugin;
        }

        return null;
    }
}
