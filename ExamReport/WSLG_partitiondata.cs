using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ExamReport
{
    class WSLG_partitiondata : PartitionData
    {
        public decimal discriminant;
        public List<decimal> total_discriminant = new List<decimal>();
        public List<decimal> group_discriminant = new List<decimal>();
        public int PLN;
        public int PHN;
        public List<Disc> total = new List<Disc>();
        public List<Disc> group = new List<Disc>();

        public WSLG_partitiondata(string title) : base(title)
        {
        }

        public class Disc
        {
            decimal PLN;
            decimal PHN;

            int count;
            decimal fullmark;

            public Disc(int _count, decimal _fullmark)
            {
                PLN = 0m;
                PHN = 0m;
                count = _count;
                fullmark = _fullmark;
            }

            public void AddData(decimal value, bool isPLN)
            {
                if (isPLN)
                    PLN += value;
                else
                    PHN += value;
            }
            public decimal GetAns()
            {
                if (count > 0)
                    return ((PHN - PLN) / count) / fullmark;
                else
                    return 0;
            }
        }
    }
}
