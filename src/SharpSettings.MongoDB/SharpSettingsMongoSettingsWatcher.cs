using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using KellermanSoftware.CompareNetObjects;
using KellermanSoftware.CompareNetObjects.TypeComparers;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;

[assembly:InternalsVisibleTo("SharpSettings.MongoDB.Tests")]
namespace SharpSettings.MongoDB
{
    public class SharpSettingsMongoSettingsWatcher<TSettingsObject> : ISettingsWatcher<string, TSettingsObject> where TSettingsObject : WatchableSettings<string>
    {
        internal Task _watcherTask;

        private CancellationTokenSource _cancellationTokenSource;
        private bool _startupComplete;
        private TSettingsObject _settings;

        private readonly bool _forcePolling;
        private readonly string _settingsId;
        private readonly CompareLogic _compareLogic;
        private readonly SharpSettingsMongoDataStore<TSettingsObject> _store;
        private readonly Action<TSettingsObject> _settingsUpdatedCallback;
        private readonly ILogger _logger;

        /// <summary>
        /// Create a new <see cref="SharpSettingsMongoSettingsWatcher{TSettingsObject}"/>
        /// </summary>
        /// <param name="logger">A <see cref="ILogger"/> for capturing internal log messages.</param>
        /// <param name="settingsStore">A <see cref="SharpSettingsMongoDataStore{TSettingsObject}"/> that contains the <see cref="TSettingsObject"/></param>
        /// <param name="settings">The <see cref="TSettingsObject"/> to monitor</param>
        /// <param name="settingsUpdatedCallback">A callback for notifying the creator of this object of updates to the <see cref="TSettingsObject"/></param>
        /// <param name="forcePolling">A <see cref="bool"/> value indicating if Polling should be used instead of auto-selecting between Polling and ChangeStreams</param>
        /// <param name="customComparers">A <see cref="IEnumerable{BaseTypeComparer}"/> for determining when the <see cref="TSettingsObject"/> has changed. This is not required if ChangeStreams are used.</param>
        /// <param name="cts">A <see cref="CancellationTokenSource"/> for shutting down the <see cref="ISettingsWatcher{TId, TSettings}"/> when not using the <see cref="IAsyncDisposable"/> pattern.</param>
        public SharpSettingsMongoSettingsWatcher(ILogger logger, SharpSettingsMongoDataStore<TSettingsObject> settingsStore, TSettingsObject settings,
            Action<TSettingsObject> settingsUpdatedCallback, bool forcePolling = false,
            IEnumerable<BaseTypeComparer> customComparers = null, CancellationTokenSource cts = default)
            : this(logger, settingsStore, settings.Id, settingsUpdatedCallback, forcePolling, customComparers, cts)
        {
        }

        /// <summary>
        /// Create a new <see cref="SharpSettingsMongoSettingsWatcher{TSettingsObject}"/>
        /// </summary>
        /// <param name="logger">A <see cref="ILogger"/> for capturing internal log messages.</param>
        /// <param name="settingsStore">A <see cref="SharpSettingsMongoDataStore{TSettingsObject}"/> that contains the <see cref="TSettingsObject"/></param>
        /// <param name="settingsId">The <see cref="string"/> Id of the <see cref="TSettingsObject{string}"/> to monitor.</param>
        /// <param name="settingsUpdatedCallback">A callback for notifying the creator of this object of updates to the <see cref="TSettingsObject"/></param>
        /// <param name="forcePolling">A <see cref="bool"/> value indicating if Polling should be used instead of auto-selecting between Polling and ChangeStreams</param>
        /// <param name="customComparers">A <see cref="IEnumerable{BaseTypeComparer}"/> for determining when the <see cref="TSettingsObject"/> has changed. This is not required if ChangeStreams are used.</param>
        /// <param name="cts">A <see cref="CancellationTokenSource"/> for shutting down the <see cref="ISettingsWatcher{TId, TSettings}"/> when not using the <see cref="IAsyncDisposable"/> pattern.</param>
        public SharpSettingsMongoSettingsWatcher(ILogger logger, SharpSettingsMongoDataStore<TSettingsObject> settingsStore, string settingsId,
            Action<TSettingsObject> settingsUpdatedCallback, bool forcePolling = false,
            IEnumerable<BaseTypeComparer> customComparers = null, CancellationTokenSource cts = default)
        {
            _logger = logger;
            _cancellationTokenSource = cts ?? new CancellationTokenSource();
            _forcePolling = forcePolling;
            _compareLogic = new CompareLogic();
            if (customComparers != null)
                _compareLogic.Config.CustomComparers.AddRange(customComparers);
            _store = settingsStore;
            _settingsId = settingsId;
            _settingsUpdatedCallback = settingsUpdatedCallback;
            CreateWatcherTask();
        }

        private void CreateWatcherTask()
        {
            if (_store.Store.Database.Client.Cluster.Description.Type == ClusterType.ReplicaSet &&
                _forcePolling == false)
            {
                _logger.LogDebug("Calling start on a Watcher task.");
                _watcherTask = Task.Factory.StartNew(() => ChangeStream(_cancellationTokenSource.Token), _cancellationTokenSource.Token, TaskCreationOptions.LongRunning,
                    TaskScheduler.Default).Unwrap();
                _logger.LogDebug("Finished calling start on a Watcher task.");
            }
            else
            {
                _logger.LogDebug("Calling start on a Polling task.");
                _watcherTask = Task.Factory.StartNew(() => PollAsync(_cancellationTokenSource.Token), _cancellationTokenSource.Token, TaskCreationOptions.LongRunning,
                    TaskScheduler.Default).Unwrap();
                _logger.LogDebug("Finished calling start on a Polling task.");
            }
        }

        private async Task PollAsync(CancellationToken cancellationToken)
        {
            _logger.LogTrace("Starting a Polling task.");
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    if (_store.Store.Database.Client.Cluster.Description.Type == ClusterType.ReplicaSet && !_forcePolling)
                    {
                        _startupComplete = false;
                        _logger.LogTrace("Detected change to replica set. Changing to OpLog Tail.");

                        _watcherTask = Task.Factory.StartNew(() => ChangeStream(cancellationToken), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
                        break;
                    }
                    var tmpSettings = await _store.FindAsync(_settingsId).ConfigureAwait(false);
                    _startupComplete = true;

                    if (_compareLogic.Compare(tmpSettings, _settings).AreEqual == false)
                    {
                        _logger.LogTrace("Settings updated.");

                        _settings = tmpSettings;
                        _settingsUpdatedCallback?.Invoke(_settings);

                        _logger.LogTrace("SettingsWatcher notified.");
                    }
                    if (_settings == null)
                    {
                        _logger.LogWarning("Settings not found.");
                    }
                }
                catch (TaskCanceledException)
                {
                    _logger.LogDebug("Watcher task was cancelled.");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "An exception occurred in the polling task.");
                }
                await Task.Delay(500, cancellationToken).ConfigureAwait(false);
            }
            _logger.LogTrace("Ending a Polling task.");
        }

        private async Task ChangeStream(CancellationToken cancellationToken)
        {
            _logger.LogTrace("Starting a Tailing task.");
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    if (_store.Store.Database.Client.Cluster.Description.Type != ClusterType.ReplicaSet)
                    {
                        _startupComplete = false;
                        _logger.LogTrace("Detected change to non-replica set. Changing to Poll.");
                        _watcherTask = Task.Factory.StartNew(() => PollAsync(cancellationToken), cancellationToken, TaskCreationOptions.LongRunning,
                            TaskScheduler.Default).Unwrap();
                        break;
                    }

                    // Perform the initial settings load.
                    _settings = await _store.FindAsync(_settingsId);
                    _settingsUpdatedCallback?.Invoke(_settings);
                    _startupComplete = true;

                    var changeStream = await _store.Store.WatchAsync(new ChangeStreamOptions()
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                    }, cancellationToken);
                    _logger.LogTrace("Created Change Stream watcher.");
                    while (await changeStream.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    {
                        _logger.LogTrace($"Received Change Stream batch. Docs in batch {changeStream.Current.Count()}");
                        foreach (var doc in changeStream.Current)
                        {
                            if (doc.FullDocument == null) continue;
                            if (doc.FullDocument.Id != _settingsId) continue;

                            _logger.LogTrace("Settings update received, invoking callback.");

                            _settings = doc.FullDocument;
                            _settingsUpdatedCallback?.Invoke(_settings);

                            _logger.LogTrace("Callback invoked.");
                        }
                    }
                    _logger.LogTrace("Change Stream watcher ended, restarting...");
                }
                catch (TaskCanceledException)
                {
                    _logger.LogDebug("Watcher task was cancelled.");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "An error occurred in the changestream. Cancelled? {cancelled}", cancellationToken.IsCancellationRequested);
                    _logger.LogTrace("Change Stream watcher ended due to an exception, restarting...");
                }
            }
            _logger.LogTrace("Ending a Tailing task.");
        }

        /// <summary>
        /// Dispose of this <see cref="ISettingsWatcher{TId, TSettings}"/> object
        /// </summary>
        public void Dispose()
        {
            DisposeAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }
        
        /// <summary>
        /// Asynchronously dispose of this <see cref="ISettingsWatcher{TId, TSettings}"/> object
        /// </summary>
        /// <returns>A <see cref="ValueTask"/> representing the dispose operation.</returns>
        public async ValueTask DisposeAsync()
        {
            _cancellationTokenSource.Cancel(false);
            await _watcherTask.ConfigureAwait(false);
        }

        /// <summary>
        /// Get the <see cref="TSettings"/> object
        /// </summary>
        /// <returns>The <see cref="TSettings"/> object if available, otherwise null</returns>
        public TSettingsObject GetSettings()
        {
            return _settings;
        }

        /// <summary>
        /// Wait for the <see cref="TSettings"/> object to become available and get it
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async ValueTask<TSettingsObject> GetSettingsAsync(CancellationToken cancellationToken = default)
        {
            var settingsIteration = 0;
            while (_settings == null && !cancellationToken.IsCancellationRequested)
            {
                _logger.LogDebug($"Settings null after {settingsIteration} iterations.");
                await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                settingsIteration++;
            }

            return _settings;
        }

        /// <summary>
        /// Restart the <see cref="ISettingsWatcher{TId, TSettings}"/>. Safe to use in both faulted and unfaulted states.
        /// Blocks until startup has completed.
        /// </summary>
        /// <param name="timeout">The timeout in milliseconds, set to -1 for no timeout</param>
        /// <returns>A <see cref="bool"/> value indicating if the restart was successful within the <paramref name="timeout"/>.</returns>
        public bool Restart(int timeout)
        {
            return RestartAsync(timeout).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously restart the <see cref="ISettingsWatcher{TId, TSettings}"/>. Safe to use in both faulted and unfaulted states.
        /// Blocks until startup has completed.
        /// </summary>
        /// <param name="timeout">The timeout in milliseconds, set to -1 for no timeout</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> to cancel the operation</param>
        /// <returns>A <see cref="bool"/> value indicating if the restart was successful within the <paramref name="timeout"/>.</returns>
        public async ValueTask<bool> RestartAsync(int timeout, CancellationToken cancellationToken = default)
        {
            try
            {
                _cancellationTokenSource.Cancel();
                await _watcherTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Caught expected exception while restarting the watcher.");
            }
            _cancellationTokenSource = new CancellationTokenSource();
            CreateWatcherTask();
            return await WaitForStartupAsync(timeout, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Synchronously wait for the <see cref="ISettingsWatcher{TId, TSettings}"/> to startup.
        /// Since the <see cref="ISettingsWatcher{TId, TSettings}"/> may require network I/O and significant work
        /// to provide the settings, it may require some amount of time to startup. If applications must block
        /// until this infrastructure is setup, call this method.
        /// </summary>
        /// <param name="timeout">The timeout in milliseconds, set to 0 for no timeout</param>
        /// <returns>A <see cref="bool"/> value indicating if the startup was successful within the <paramref name="timeout"/>.</returns>
        public bool WaitForStartup(int timeout)
        {
            return WaitForStartupAsync(timeout).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Wait for the <see cref="ISettingsWatcher{TId, TSettings}"/> to startup.
        /// Since the <see cref="ISettingsWatcher{TId, TSettings}"/> may require network I/O and significant work
        /// to provide the settings, it may require some amount of time to startup. If applications must block
        /// until this infrastructure is setup, <see langword="await"/> this method.
        /// </summary>
        /// <param name="timeout">The timeout in milliseconds, set to 0 for no timeout</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> to cancel the operation</param>
        /// <returns>A <see cref="bool"/> value indicating if the startup was successful within the <paramref name="timeout"/>.</returns>
        public ValueTask<bool> WaitForStartupAsync(int timeout, CancellationToken cancellationToken = default)
        {
            var timespan = timeout == 0 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(timeout);
            return WaitForStartupAsync(timespan, cancellationToken);
        }

        /// <summary>
        /// Synchronously wait for the <see cref="ISettingsWatcher{TId, TSettings}"/> to startup.
        /// Since the <see cref="ISettingsWatcher{TId, TSettings}"/> may require network I/O and significant work
        /// to provide the settings, it may require some amount of time to startup. If applications must block
        /// until this infrastructure is setup, call this method.
        /// </summary>
        /// <param name="timeout">The amount of time to wait for startup, set to <see cref="TimeSpan.Zero"/> for no timeout</param>
        /// <returns>A <see cref="bool"/> value indicating if the startup was successful within the <paramref name="timeout"/>.</returns>
        public bool WaitForStartup(TimeSpan timeout)
        {
            return WaitForStartupAsync(timeout).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Wait for the <see cref="ISettingsWatcher{TId, TSettings}"/> to startup.
        /// Since the <see cref="ISettingsWatcher{TId, TSettings}"/> may require network I/O and significant work
        /// to provide the settings, it may require some amount of time to startup. If applications must block
        /// until this infrastructure is setup, <see langword="await"/> this method.
        /// </summary>
        /// <param name="timeout">The amount of time to wait for startup, set to <see cref="TimeSpan.Zero"/> for no timeout</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> to cancel the operation</param>
        /// <returns>A <see cref="bool"/> value indicating if the startup was successful within the <paramref name="timeout"/>.</returns>
        public async ValueTask<bool> WaitForStartupAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            DateTime endTime;
            if (timeout == TimeSpan.Zero)
            {
                endTime = DateTime.MaxValue;
            }
            else 
                endTime = DateTime.UtcNow.Add(timeout);
            while(_startupComplete == false && endTime > DateTime.UtcNow)
            {
                await Task.Delay(10, cancellationToken).ConfigureAwait(false);
            }
            return _startupComplete;
        }

        /// <summary>
        /// A value indicating if an internal fault has stopped the <see cref="ISettingsWatcher{TId, TSettings}"/>
        /// </summary>
        /// <returns>A <see cref="bool"/> value indicating the fault state.</returns>
        public bool IsFaulted()
        {
            return _watcherTask == null 
                ? true 
                : _watcherTask.Status == TaskStatus.Faulted || _watcherTask.Status == TaskStatus.Canceled;
        }

        /// <summary>
        /// A value indicating if the <see cref="ISettingsWatcher{TId, TSettings}"/> is is running
        /// </summary>
        /// <returns>A <see cref="bool"/> value indicating the internal run state.</returns>
        public bool IsRunning()
        {
            return _watcherTask == null 
                ? false 
                : (_watcherTask.Status == TaskStatus.WaitingForActivation 
                    || _watcherTask.Status == TaskStatus.Running 
                    || _watcherTask.Status == TaskStatus.WaitingToRun) 
                && _startupComplete;

        }
    }
}
