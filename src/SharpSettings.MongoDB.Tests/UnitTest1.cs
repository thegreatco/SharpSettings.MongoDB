using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;

namespace SharpSettings.MongoDB.Tests
{
    [TestClass]
    public class UnitTest1
    {
        private static SharpSettingsMongoDataStore<TestSettings> _testSettingsDataStore;
        private static readonly MongoClient MongoClient = new MongoClient();
        private static readonly IMongoDatabase Db = MongoClient.GetDatabase(nameof(UnitTest1));
        private static readonly IMongoCollection<TestSettings> Col = Db.GetCollection<TestSettings>("settings");
        private static readonly TestSharpSettingsLogger SharpSettingsLogger = new TestSharpSettingsLogger();

        [ClassInitialize]
        public static void Setup(TestContext context)
        {
            _testSettingsDataStore = new SharpSettingsMongoDataStore<TestSettings>(Col, SharpSettingsLogger);
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            MongoClient.DropDatabase(nameof(UnitTest1));
        }

        [TestMethod]
        public async Task TestInsertIsCaptured()
        {
            while (MongoClient.Cluster.Description.Type != ClusterType.ReplicaSet)
            {
                await Task.Delay(1000);
            }

            TestSettings testSettings = null;

            void SettingsCallback(TestSettings settings)
            {
                testSettings = settings;
                SharpSettingsLogger.Information(settings.ToJson());
            }

            using (var settingsWatcher = new SharpSettingsMongoSettingsWatcher<TestSettings>(_testSettingsDataStore, "Local", SettingsCallback))
            {
                Assert.IsNull(testSettings);
                var newSettings = new TestSettings() {Foo = "foo", Bar = "bar", Id = "Local", LastUpdate = 0};
                await Col.InsertOneAsync(newSettings);
                var waitCount = 0;
                while (testSettings == null && waitCount < 10)
                {
                    await Task.Delay(1000);
                    waitCount++;
                }
                Assert.IsNotNull(testSettings);
            }
        }

        [TestMethod]
        public async Task TestUpdateIsCaptured()
        {
            while (MongoClient.Cluster.Description.Type != ClusterType.ReplicaSet)
            {
                await Task.Delay(1000);
            }
            var newSettings = new TestSettings() { Foo = "Foo", Bar = "Bar", Id = "Local", LastUpdate = 0 };
            await Col.InsertOneAsync(newSettings);

            TestSettings testSettings = null;
            void SettingsCallback(TestSettings settings)
            {
                testSettings = settings;
                SharpSettingsLogger.Information(settings.ToJson());
            }

            using (var settingsWatcher = new SharpSettingsMongoSettingsWatcher<TestSettings>(_testSettingsDataStore, "Local", SettingsCallback))
            {
                Assert.IsNull(testSettings);
                await Col.UpdateOneAsync(Builders<TestSettings>.Filter.Eq(x => x.Id, newSettings.Id),
                    Builders<TestSettings>.Update.Set(x => x.Foo, "Bar").Set(x => x.Bar, "Foo"));
                var waitCount = 0;
                while (testSettings == null && waitCount < 10)
                {
                    await Task.Delay(1000);
                    waitCount++;
                }
                Assert.IsNotNull(testSettings);
                Assert.AreEqual(testSettings.Bar, "Foo");
                Assert.AreEqual(testSettings.Foo, "Bar");
            }
        }
    }
}
