using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace StopWatcher
{
    public interface ITestCase
    {
        Task RunAsync();
        Task PrintResultAsync();
        Task SaveResultAsync();
    }

    public abstract class AbstractTestCase : ITestCase
    {
        public virtual async Task RunAsync()
        {
            try
            {
                Console.WriteLine($"> Before {nameof(DoWorkAsync)}");
                await DoWorkAsync();
                Console.WriteLine($"> After {nameof(DoWorkAsync)}");

                try
                {
                    await PrintResultAsync();
                    Console.WriteLine($"> After {nameof(PrintResultAsync)}");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Console.WriteLine("Error occured while trying to print result");
                }

                await SaveResultAsync();
                Console.WriteLine($"> After {nameof(SaveResultAsync)}");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        protected abstract Task DoWorkAsync();

        public abstract Task PrintResultAsync();
        public abstract Task SaveResultAsync();

        private readonly DateTime _runDate = DateTime.Now;
        protected string FileResult => $"{GetType().Name}_{_runDate:HH-mm-ss}.txt";

        protected Task WriteResultAsync(string contents) => File.WriteAllTextAsync(FileResult, contents, Encoding.UTF8);
        protected Task AppendResultAsync(string contents) => File.AppendAllTextAsync(FileResult, contents, Encoding.UTF8);
    }
}