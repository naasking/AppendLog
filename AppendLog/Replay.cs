using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AppendLog
{
    public struct Replay
    {
        TransactionId transaction;

        public Replay(TransactionId transaction)
            : this()
        {
            this.transaction = transaction;
        }

        public Replay(IAsyncResult op)
            : this()
        {
            this.Operation = op;
        }

        public IAsyncResult Operation { get; set; }

        public bool TryGetValue(out TransactionId transaction)
        {
            if (Operation == null)
            {
                transaction = this.transaction;
                return true;
            }
            else
            {
                transaction = default(TransactionId);
                return false;
            }
        }
    }
}
