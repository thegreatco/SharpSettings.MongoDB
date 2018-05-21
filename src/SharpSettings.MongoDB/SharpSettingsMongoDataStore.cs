using System.Threading.Tasks;
using MongoDB.Driver;
using SharpSettings;

namespace SharpSettings.MongoDB
{
    public class SharpSettingsMongoDataStore<TSettingsObject> : ISharpSettingsDataStore<string, TSettingsObject>
        where TSettingsObject : WatchableSettings<string>
    {
        internal readonly IMongoCollection<TSettingsObject> Store;
        internal readonly ISharpSettingsLogger Logger;

        public SharpSettingsMongoDataStore(IMongoCollection<TSettingsObject> store, ISharpSettingsLogger logger = null)
        {
            Store = store;
            Logger = logger;
        }

        public async Task<TSettingsObject> FindAsync(string settingsId)
        {
            return await Store.Find(Builders<TSettingsObject>.Filter.Eq(x => x.Id, settingsId)).SingleOrDefaultAsync();
        }

        public TSettingsObject Find(string settingsId)
        {
            return Store.Find(Builders<TSettingsObject>.Filter.Eq(x => x.Id, settingsId)).SingleOrDefault();
        }
    }
}