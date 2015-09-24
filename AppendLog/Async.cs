using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AppendLog
{
    public struct Async<T>
    {
        T value;

        public Async(T value)
            : this()
        {
            this.value = value;
        }

        public Async(IAsyncResult op)
            : this()
        {
            this.Operation = op;
        }

        public IAsyncResult Operation { get; set; }

        public bool TryGetValue(out T value)
        {
            if (Operation == null)
            {
                value = this.value;
                return true;
            }
            else
            {
                value = default(T);
                return false;
            }
        }
    }
}
