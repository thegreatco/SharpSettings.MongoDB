using System;
using System.Collections.Generic;
using System.Text;

namespace SharpSettings.MongoDB.Tests
{
    public class TestSharpSettingsLogger : ISharpSettingsLogger
    {
        public void Critical(string message)
        {
            Console.WriteLine(message);
        }

        public void Critical(Exception ex)
        {
            Console.WriteLine(ex);
        }

        public void Critical(Exception ex, string message)
        {
            Console.WriteLine($"{message} - {ex}");
        }

        public void Debug(string message)
        {
            Console.WriteLine(message);
        }

        public void Debug(Exception ex)
        {
            Console.WriteLine(ex);
        }

        public void Debug(Exception ex, string message)
        {
            Console.WriteLine($"{message} - {ex}");
        }

        public void Error(string message)
        {
            Console.WriteLine(message);
        }

        public void Error(Exception ex)
        {
            Console.WriteLine(ex);
        }

        public void Error(Exception ex, string message)
        {
            Console.WriteLine($"{message} - {ex}");
        }

        public void Information(string message)
        {
            Console.WriteLine(message);
        }

        public void Information(Exception ex)
        {
            Console.WriteLine(ex);
        }

        public void Information(Exception ex, string message)
        {
            Console.WriteLine($"{message} - {ex}");
        }

        public void Trace(string message)
        {
            Console.WriteLine(message);
        }

        public void Trace(Exception ex)
        {
            Console.WriteLine(ex);
        }

        public void Trace(Exception ex, string message)
        {
            Console.WriteLine($"{message} - {ex}");
        }

        public void Warn(string message)
        {
            Console.WriteLine(message);
        }

        public void Warn(Exception ex)
        {
            Console.WriteLine(ex);
        }

        public void Warn(Exception ex, string message)
        {
            Console.WriteLine($"{message} - {ex}");
        }
    }
}
