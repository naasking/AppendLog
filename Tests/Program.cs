using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AppendLog;

namespace Tests
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var fl = new FileLog("test.db"))
            {
                using (var buf = fl.Append(false))
                    buf.Write(Encoding.ASCII.GetBytes("hello"), 0, 5);
                var tmp = new byte[1024];
                var ie = fl.Replay(fl.First);
                while (ie.MoveNext().Result)
                {
                    var i = ie.Stream.Read(tmp, 0, tmp.Length);
                    Console.WriteLine(Encoding.ASCII.GetString(tmp, 0, i));
                }
            }
        }
    }
}
