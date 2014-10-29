using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace ExamReport
{
    class ZF_worddata
    {
        public int total_num;
        public decimal fullmark;
        public decimal max;
        public decimal min;
        public decimal avg;
        public decimal stDev;
        public decimal Dfactor;
        public decimal difficulty;

        public DataTable dist;
        public DataTable frequency;

        public ZF_worddata()
        {
            total_num = 0;
            fullmark = 0m;
            max = 0m;
            min = 0m;
            avg = 0m;
            stDev = 0m;
            Dfactor = 0m;
            difficulty = 0m;

            dist = new DataTable();
            frequency = new DataTable();

            frequency.Columns.Add("totalmark", typeof(decimal));
            frequency.Columns.Add("frequency", typeof(int));
            frequency.Columns.Add("rate", typeof(decimal));
            frequency.Columns.Add("accumulateFreq", typeof(int));
            frequency.Columns.Add("accumulateRate", typeof(decimal));
            frequency.PrimaryKey = new DataColumn[] { frequency.Columns["totalmark"] };

            dist.Columns.Add("mark", typeof(decimal));
            dist.Columns.Add("rate", typeof(decimal));

            
        }
        
    }
}
