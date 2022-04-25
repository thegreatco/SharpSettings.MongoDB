# SharpSettings
A implementation of SharpSettings that stores and retrieves settings from a MongoDB database.

| dev | master |
| --- | ------ |
| [![Build and Test](https://github.com/thegreatco/SharpSettings.MongoDB/actions/workflows/build.yml/badge.svg?branch=dev)](https://github.com/thegreatco/SharpSettings.MongoDB/actions/workflows/build.yml) | [![Build and Test](https://github.com/thegreatco/SharpSettings.MongoDB/actions/workflows/build.yml/badge.svg)](https://github.com/thegreatco/SharpSettings.MongoDB/actions/workflows/build.yml)|

See [SharpSetting](https://github.com/thegreatco/SharpSettings) for general usage instructions.
# Usage

WIP

### Logger
To be as flexible as possible and not requiring a particular logging framework, a shim must be implemented that implements the `ISharpSettingsLogger` interface. It follows similar patterns to `Serilog.ILogger` but is easily adapted to `Microsoft.Extensions.Logging` as well.
