using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace ExamReport
{
    public class WordData
    {
        public int total_num;
        public decimal fullmark;
        public decimal max;
        public decimal min;
        public decimal avg;
        public decimal stDev;
        public decimal Dfactor;
        public decimal difficulty;
        public decimal alfa;
        public decimal standardErr;
        /// <summary>
        /// 中数
        /// </summary>
        public decimal mean;
        /// <summary>
        /// 众数
        /// </summary>
        public decimal mode;
        /// <summary>
        /// 偏度
        /// </summary>
        public decimal skewness;
        /// <summary>
        /// 峰度
        /// </summary>
        public decimal kertosis;
        public DataTable total_analysis;
        public DataTable group_analysis;
        public DataTable frequency_dist;
        public enum single_type { single, multiple, sub };
        public DataTable topicDifficultyTable;

        public DataTable totalmark_dist;

        public class group_data
        {
            public DataTable group_detail = new DataTable();
            public DataTable group_dist = new DataTable();
            public DataTable group_difficulty = new DataTable();
        }
        public ArrayList single_group_analysis;

        public class single_data
        {
            
            public single_type stype;
            public DataTable single_difficulty = new DataTable();
            public DataTable single_dist = new DataTable();
            public DataTable single_detail = new DataTable();

        }
        public ArrayList single_topic_analysis;
        public DataTable _standard_ans;
        public DataTable _groups_ans;

        public List<DataTable> group_cor;
        public Dictionary<string, List<string>> groups_group;
        public WordData(Dictionary<string, List<string>> _groups_group)
        {
            groups_group = _groups_group;
            total_num = 0;
            fullmark = 0.0m;
            max = 0.0m;
            min = 0.0m;
            avg = 0.0m;
            stDev = 0.0m;
            Dfactor = 0.0m;
            difficulty = 0.0m;
            alfa = 0.0m;
            standardErr = 0.0m;
            mean = 0.0m;
            mode = 0.0m;
            skewness = 0.0m;
            kertosis = 0.0m;
            total_analysis = new DataTable();
            group_analysis = new DataTable();
            frequency_dist = new DataTable();
            single_group_analysis = new ArrayList();
            single_topic_analysis = new ArrayList();

            totalmark_dist = new DataTable();

            totalmark_dist.Columns.Add("mark", typeof(decimal));
            totalmark_dist.Columns.Add("num", typeof(int));


            total_analysis.Columns.Add("number",typeof(string));
            total_analysis.Columns.Add("fullmark", typeof(decimal));
            total_analysis.Columns.Add("max", typeof(decimal));
            total_analysis.Columns.Add("min", typeof(decimal));
            total_analysis.Columns.Add("avg", typeof(decimal));
            total_analysis.Columns.Add("standardErr", typeof(decimal));
            total_analysis.Columns.Add("dfactor", typeof(decimal));
            total_analysis.Columns.Add("difficulty", typeof(decimal));
            total_analysis.Columns.Add("correlation", typeof(decimal));
            total_analysis.Columns.Add("discriminant", typeof(decimal));
            total_analysis.Columns.Add("PHN", typeof(decimal));
            total_analysis.Columns.Add("PLN", typeof(decimal));
            total_analysis.Columns.Add("CorrectMark", typeof(decimal));
            total_analysis.Columns.Add("CorrectNum", typeof(decimal));
            total_analysis.Columns.Add("WrongMark", typeof(decimal));
            total_analysis.Columns.Add("WrongNum", typeof(decimal));
            total_analysis.Columns.Add("MultipleSum", typeof(decimal));
            total_analysis.Columns.Add("SquareSumX", typeof(decimal));
            total_analysis.Columns.Add("objective", typeof(int));
            total_analysis.PrimaryKey = new DataColumn[] { total_analysis.Columns["number"] };

            group_analysis.Columns.Add("number", typeof(string));
            group_analysis.Columns.Add("fullmark", typeof(decimal));
            group_analysis.Columns.Add("max", typeof(decimal));
            group_analysis.Columns.Add("min", typeof(decimal));
            group_analysis.Columns.Add("avg", typeof(decimal));
            group_analysis.Columns.Add("standardErr", typeof(decimal));
            group_analysis.Columns.Add("dfactor", typeof(decimal));
            group_analysis.Columns.Add("difficulty", typeof(decimal));
            group_analysis.Columns.Add("correlation", typeof(decimal));
            group_analysis.Columns.Add("discriminant", typeof(decimal));
            group_analysis.Columns.Add("PHN", typeof(decimal));
            group_analysis.Columns.Add("PLN", typeof(decimal));
            group_analysis.Columns.Add("MultipleSum", typeof(decimal));
            group_analysis.Columns.Add("SquareSumX", typeof(decimal));
            group_analysis.PrimaryKey = new DataColumn[] { group_analysis.Columns["number"] };

            frequency_dist.Columns.Add("totalmark", typeof(decimal));
            frequency_dist.Columns.Add("frequency", typeof(int));
            frequency_dist.Columns.Add("rate", typeof(decimal));
            frequency_dist.Columns.Add("accumulateFreq", typeof(int));
            frequency_dist.Columns.Add("accumulateRate", typeof(decimal));

            group_cor = new List<DataTable>();
        }


    }
}
