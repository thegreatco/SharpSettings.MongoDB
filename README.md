# SharpSettings
A implementation of SharpSettings that stores and retrieves settings from a MongoDB database.

| dev | master |
| --- | ------ |
| [![CircleCI](https://circleci.com/gh/thegreatco/SharpSettings.MongoDB/tree/dev.svg?style=svg)](https://circleci.com/gh/thegreatco/SharpSettings.MongoDB/tree/dev) | [![CircleCI](https://circleci.com/gh/thegreatco/SharpSettings.MongoDB/tree/master.svg?style=svg)](https://circleci.com/gh/thegreatco/SharpSettings.MongoDB/tree/master) |

See [SharpSetting](https://github.com/thegreatco/SharpSettings) for general usage instructions.
# Usage

WIP

### Logger
To be as flexible as possible and not requiring a particular logging framework, a shim must be implemented that implements the `ISharpSettingsLogger` interface. It follows similar patterns to `Serilog.ILogger` but is easily adapted to `Microsoft.Extensions.Logging` as well.