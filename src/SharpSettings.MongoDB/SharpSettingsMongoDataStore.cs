using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace SharpSettings.MongoDB
{
    public class SharpSettingsMongoDataStore<TSettingsObject> : ISharpSettingsDataStore<string, TSettingsObject>
        where TSettingsObject : WatchableSettings<string>
    {
        internal readonly IMongoCollection<TSettingsObject> Store;
        private readonly ILogger _logger;

        public SharpSettingsMongoDataStore(IMongoCollection<TSettingsObject> store, ILogger logger = null)
        {
            Store = store;
            _logger = logger;
        }

        public SharpSettingsMongoDataStore(IMongoCollection<TSettingsObject> store, ILoggerFactory loggerFactory = null)
            : this (store, loggerFactory?.CreateLogger<SharpSettingsMongoDataStore<TSettingsObject>>())
        {
        }

        public async ValueTask<TSettingsObject> FindAsync(string settingsId)
        {
            _logger?.LogDebug("Retrieving settings");
            return await Store.Find(Builders<TSettingsObject>.Filter.Eq(x => x.Id, settingsId)).SingleOrDefaultAsync().ConfigureAwait(false);
        }

        public TSettingsObject Find(string settingsId)
        {
            _logger?.LogDebug("Retrieving settings");
            return Store.Find(Builders<TSettingsObject>.Filter.Eq(x => x.Id, settingsId)).SingleOrDefault();
        }
    }
}