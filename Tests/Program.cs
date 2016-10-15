using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using AppendLog;

namespace Tests
{
    class Program
    {
        static void Main(string[] args)
        {
            BasicTest();
        }

        static void BasicTest()
        {
            var fl = new FileLog("test.db");
            try
            {
                using (var buf = fl.Append(false))
                {
                    buf.Write(Encoding.ASCII.GetBytes("hello"), 0, 5);
                    buf.Write(Encoding.ASCII.GetBytes("world!"), 0, 6);
                }
                using (var buf = fl.Append(false))
                {
                    buf.Write(Encoding.ASCII.GetBytes("hello"), 0, 5);
                    buf.Write(Encoding.ASCII.GetBytes("world!"), 0, 6);
                }
                var tmp = new byte[1024];
                var count = 0;
                using (var ie = fl.Replay(fl.First))
                {
                    while (ie.MoveNext().Result)
                    {
                        using (var tr = new StreamReader(ie.Stream))
                        {
                            Debug.Assert(tr.ReadToEnd() == "helloworld!");
                            ++count;
                        }
                    }
                }
                Debug.Assert(count == 2);
            }
            finally
            {
                fl.Dispose();
                File.Delete(Path.GetFullPath("test.db"));
            }
        }
    }
}
