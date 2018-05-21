using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson;

namespace SharpSettings.MongoDB.Tests
{
    public class TestSettings : WatchableSettings<string>
    {
        public string Foo { get; set; }
        public string Bar { get; set; }
    }
}
