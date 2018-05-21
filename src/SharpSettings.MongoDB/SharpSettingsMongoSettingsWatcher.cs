using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KellermanSoftware.CompareNetObjects;
using KellermanSoftware.CompareNetObjects.TypeComparers;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;

namespace SharpSettings.MongoDB
{
    public class SharpSettingsMongoSettingsWatcher<TSettingsObject> : ISettingsWatcher<string, TSettingsObject> where TSettingsObject : WatchableSettings<string>
    {
        private readonly bool _forcePolling;
        private readonly string _settingsId;
        private readonly CompareLogic _compareLogic;
        private readonly SharpSettingsMongoDataStore<TSettingsObject> _store;
        private Task _watcherTask;
        private readonly Action<TSettingsObject> _settingsUpdatedCallback;
        private readonly CancellationTokenSource _cancellationTokenSource;

        private TSettingsObject _settings;

        public async Task<TSettingsObject> GetSettingsAsync(CancellationToken token = default(CancellationToken))
        {
            while(_settings == null && !token.IsCancellationRequested)
            {
                await Task.Delay(100, token);
            }

            return _settings;
        }

        // ReSharper disable once UnusedMember.Global
        public SharpSettingsMongoSettingsWatcher(SharpSettingsMongoDataStore<TSettingsObject> settingsStore, WatchableSettings<string> settings,
            Action<TSettingsObject> settingsUpdatedCallback, bool forcePolling = false,
            IEnumerable<BaseTypeComparer> customComparers = null, CancellationTokenSource cts = default(CancellationTokenSource))
            : this(settingsStore, settings.Id, settingsUpdatedCallback, forcePolling, customComparers, cts)
        {
        }

        public SharpSettingsMongoSettingsWatcher(SharpSettingsMongoDataStore<TSettingsObject> settingsStore, string settingsId,
            Action<TSettingsObject> settingsUpdatedCallback, bool forcePolling = false,
            IEnumerable<BaseTypeComparer> customComparers = null, CancellationTokenSource cts = default(CancellationTokenSource))
        {
            _cancellationTokenSource = cts;
            _forcePolling = forcePolling;
            _compareLogic = new CompareLogic();
            if (customComparers != null)
                _compareLogic.Config.CustomComparers.AddRange(customComparers);
            _store = settingsStore;
            _settingsId = settingsId;
            _settingsUpdatedCallback = settingsUpdatedCallback;
            if (_store.Store.Database.Client.Cluster.Description.Type == ClusterType.ReplicaSet &&
                forcePolling == false)
            {
                _store.Logger?.Debug("Calling start on a Watcher task.");
                _watcherTask = Task.Factory.StartNew(TailAsync, _cancellationTokenSource.Token, TaskCreationOptions.LongRunning,
                    TaskScheduler.Default);
                _store.Logger?.Debug("Finished calling start on a Watcher task.");
            }
            else
            {
                _store.Logger?.Debug("Calling start on a Polling task.");
                _watcherTask = Task.Factory.StartNew(PollAsync, _cancellationTokenSource.Token, TaskCreationOptions.LongRunning,
                    TaskScheduler.Default);
                _store.Logger?.Debug("Finished calling start on a Polling task.");
            }
        }

        private async Task PollAsync()
        {
            _store.Logger?.Trace("Starting a Polling task.");
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    if (_store.Store.Database.Client.Cluster.Description.Type == ClusterType.ReplicaSet && !_forcePolling)
                    {
                        _store.Logger?.Trace("Detected change to replica set. Changing to OpLog Tail.");

                        _watcherTask = Task.Factory.StartNew(TailAsync, _cancellationTokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                        break;
                    }
                    var tmpSettings = await _store.FindAsync(_settingsId);

                    if (_compareLogic.Compare(tmpSettings, _settings).AreEqual == false)
                    {
                        _store.Logger?.Trace("Settings updated.");

                        _settings = tmpSettings;
                        _settingsUpdatedCallback?.Invoke(_settings);

                        _store.Logger?.Trace("SettingsWatcher notified.");
                    }
                    if (_settings == null)
                    {
                        _store.Logger?.Warn("Settings not found.");
                    }
                }
                catch (Exception ex)
                {
                    _store.Logger?.Error(ex);
                }
                await Task.Delay(500);
            }
            _store.Logger?.Trace("Ending a Polling task.");
        }

        private async Task TailAsync()
        {
            _store.Logger?.Trace("Starting a Tailing task.");
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    if (_store.Store.Database.Client.Cluster.Description.Type != ClusterType.ReplicaSet)
                    {
                        _store.Logger?.Trace("Detected change to non-replica set. Changing to Poll.");
                        _watcherTask = Task.Factory.StartNew(PollAsync, _cancellationTokenSource.Token, TaskCreationOptions.LongRunning,
                            TaskScheduler.Default);
                        break;
                    }

                    var changeStream = await _store.Store.WatchAsync(new ChangeStreamOptions()
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                    }, _cancellationTokenSource.Token);
                    _store.Logger?.Trace("Created Change Stream watcher.");
                    while (await changeStream.MoveNextAsync(_cancellationTokenSource.Token))
                    {
                        _store.Logger?.Trace($"Received Change Stream batch. Docs in batch {changeStream.Current.Count()}");
                        foreach (var doc in changeStream.Current)
                        {
                            if (doc.FullDocument == null) continue;
                            if (doc.FullDocument.Id != _settingsId) continue;
                            
                            _store.Logger?.Trace("Settings update received, invoking callback.");
                            
                            _settings = doc.FullDocument;
                            _settingsUpdatedCallback?.Invoke(_settings);

                            _store.Logger?.Trace("Callback invoked.");
                        }
                    }
                    _store.Logger?.Trace("Change Stream watcher ended, restarting...");
                }
                catch (Exception ex)
                {
                    _store.Logger?.Error(ex);
                }
            }
            _store.Logger?.Trace("Ending a Tailing task.");
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel(false);
            _watcherTask?.Wait(TimeSpan.FromSeconds(10));
        }
    }
}
