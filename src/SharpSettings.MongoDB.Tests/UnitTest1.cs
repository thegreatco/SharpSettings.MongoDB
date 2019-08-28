using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using Serilog;
using Serilog.Events;
using Serilog.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace SharpSettings.MongoDB.Tests
{
    public class UnitTest1 : IDisposable
    {
        private static SharpSettingsMongoDataStore<TestSettings> _testSettingsDataStore;
        private static readonly MongoClient MongoClient = new MongoClient("mongodb://localhost:28017,localhost:28018,localhost:28019/");
        private static readonly IMongoDatabase Db = MongoClient.GetDatabase(nameof(UnitTest1));
        private static readonly IMongoCollection<TestSettings> Col = Db.GetCollection<TestSettings>("settings");
        private static ILogger _logger;

        public UnitTest1(ITestOutputHelper testOutputHelper)
        {
            var logConfig = new LoggerConfiguration()
                .WriteTo.TestOutput(testOutputHelper, LogEventLevel.Verbose)
                .MinimumLevel.Verbose();

            Log.Logger = logConfig.CreateLogger();
            ILoggerFactory loggerFactory = new SerilogLoggerFactory(Log.Logger);
            _logger = loggerFactory.CreateLogger<UnitTest1>();
            _testSettingsDataStore = new SharpSettingsMongoDataStore<TestSettings>(Col, _logger);
        }

        public void Dispose()
        {
            _logger.LogInformation("Disposing fixture");
            MongoClient.DropDatabase(nameof(UnitTest1));
        }

        [Fact]
        public async Task TestFaultedStatus()
        {
            while (MongoClient.Cluster.Description.Type != ClusterType.ReplicaSet)
            {
                await Task.Delay(1000);
            }

            TestSettings testSettings = null;

            void SettingsCallback(TestSettings settings)
            {
                testSettings = settings;
                _logger.LogInformation(settings.ToJson());
            }

            var settingsWatcher = new SharpSettingsMongoSettingsWatcher<TestSettings>(_logger, _testSettingsDataStore, "Local", SettingsCallback);
            Assert.False(settingsWatcher.IsRunning());
            Assert.False(settingsWatcher.IsFaulted());
            
            await settingsWatcher.WaitForStartupAsync(TimeSpan.FromSeconds(5));

            Assert.True(settingsWatcher.IsRunning());
            
            await settingsWatcher.DisposeAsync();
            settingsWatcher._watcherTask = null;
            Assert.False(settingsWatcher.IsRunning());
            Assert.True(settingsWatcher.IsFaulted());
        }

        [Fact]
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
                _logger.LogInformation(settings.ToJson());
            }

            await using (var settingsWatcher = new SharpSettingsMongoSettingsWatcher<TestSettings>(_logger, _testSettingsDataStore, "Local", SettingsCallback))
            {
                Assert.False(settingsWatcher.IsRunning());
                Assert.False(settingsWatcher.IsFaulted());
                
                await settingsWatcher.WaitForStartupAsync(TimeSpan.FromSeconds(5));
                
                Assert.Null(testSettings);
                var newSettings = new TestSettings() { Foo = "foo", Bar = "bar", Id = "Local", LastUpdate = 0 };
                await Col.InsertOneAsync(newSettings);
                var waitCount = 0;
                while (testSettings == null && waitCount < 10)
                {
                    await Task.Delay(1000);
                    waitCount++;
                }
                Assert.NotNull(testSettings);
            }
        }

        [Fact]
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
                _logger.LogInformation(settings.ToJson());
            }

            await using (var settingsWatcher = new SharpSettingsMongoSettingsWatcher<TestSettings>(_logger, _testSettingsDataStore, "Local", SettingsCallback))
            {
                Assert.False(settingsWatcher.IsRunning());
                Assert.False(settingsWatcher.IsFaulted());
                
                await settingsWatcher.WaitForStartupAsync(TimeSpan.FromSeconds(5));
                
                Assert.NotNull(testSettings);
                await Col.UpdateOneAsync(Builders<TestSettings>.Filter.Eq(x => x.Id, newSettings.Id),
                    Builders<TestSettings>.Update.Set(x => x.Foo, "Bar").Set(x => x.Bar, "Foo").Set(x => x.LastUpdate, 1));
                var waitCount = 0;
                while (testSettings?.LastUpdate != 1 && waitCount < 20)
                {
                    await Task.Delay(1000);
                    waitCount++;
                }
                Assert.NotNull(testSettings);
                Assert.Equal("Foo", testSettings.Bar);
                Assert.Equal("Bar", testSettings.Foo);
            }
        }

        [Fact]
        public async Task TestResultDoesNotDeadlock()
        {
            while (MongoClient.Cluster.Description.Type != ClusterType.ReplicaSet)
            {
                await Task.Delay(1000);
            }
            var newSettings = new TestSettings() { Foo = "Foo", Bar = "Bar", Id = "Local", LastUpdate = 0 };
            await Col.InsertOneAsync(newSettings);

            void SettingsCallback(TestSettings settings)
            {
                _logger.LogInformation(settings.ToJson());
            }

            await using var settingsWatcher = new SharpSettingsMongoSettingsWatcher<TestSettings>(_logger, _testSettingsDataStore, "Local", SettingsCallback);
            
            Assert.False(settingsWatcher.IsRunning());
            Assert.False(settingsWatcher.IsFaulted());
            
            await settingsWatcher.WaitForStartupAsync(TimeSpan.FromSeconds(5));

            await Col.UpdateOneAsync(Builders<TestSettings>.Filter.Eq(x => x.Id, newSettings.Id),
                Builders<TestSettings>.Update.Set(x => x.Foo, "Bar").Set(x => x.Bar, "Foo"));

            var settingses = await Task.WhenAll(Enumerable.Range(0, 10000).AsParallel().Select(x =>
            {
                return Task.Factory.StartNew(() => settingsWatcher.GetSettingsAsync().Result);
            }));
            var testSettings = settingses.First();

            Assert.NotNull(testSettings);
            Assert.Equal("Foo", testSettings.Bar);
            Assert.Equal("Bar", testSettings.Foo);
        }
    }
}
