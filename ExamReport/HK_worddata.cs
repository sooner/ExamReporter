using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace ExamReport
{
    public class HK_worddata : WordData
    {
        public DataTable total;
        public DataTable total_topic_rank;

        public List<DataTable> single_group_rank;
        public List<DataTable> single_topic_rank;

        public HK_worddata(Dictionary<string, List<string>> _groups_group):base(_groups_group)
        {

            total = new DataTable();
            total.Columns.Add("rank", typeof(string));
            total.Columns.Add("totalnum", typeof(int));
            total.Columns.Add("percent", typeof(decimal));
            total.Columns.Add("avg", typeof(decimal));
            total.Columns.Add("stDev", typeof(decimal));
            total.Columns.Add("Dfactor", typeof(decimal));
            total.Columns.Add("difficulty", typeof(decimal));

            total_topic_rank = new DataTable();
            total_topic_rank.Columns.Add("number", typeof(string));
            total_topic_rank.Columns.Add("outstanding", typeof(decimal));
            total_topic_rank.Columns.Add("good", typeof(decimal));
            total_topic_rank.Columns.Add("pass", typeof(decimal));
            total_topic_rank.Columns.Add("fail", typeof(decimal));

            single_group_rank = new List<DataTable>();
            single_topic_rank = new List<DataTable>();



        }
    }
}
