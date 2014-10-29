using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace ExamReport
{
    class ZF_statistic
    {
        DataTable _data;
        public ZF_worddata w_result;
        public ZF_worddata l_result;

        public List<DataTable> sub;
        decimal _fullmark;
        public string _name;
        public ZF_statistic(DataTable data, decimal fullmark, string name)
        {
            _data = data;
            w_result = new ZF_worddata();
            l_result = new ZF_worddata();
            _fullmark = fullmark;
            _name = name;
        }

        public void partition_process()
        {
            DataTable w_data = _data.equalfilter("type", "w");
            DataTable l_data = _data.equalfilter("type", "l");

            single_process(w_data, w_result);
            single_process(l_data, l_result);

            if (Utils.report_style.Equals("总体"))
            {
                sub = new List<DataTable>();
                insertSub(sub, _data, "yw", 150m);
                insertSub(sub, _data.equalfilter("type", "w"), "sx", 150m);
                insertSub(sub, _data.equalfilter("type", "l"), "sx", 150m);
                insertSub(sub, _data, "yy", 150m);
                insertSub(sub, _data.equalfilter("type", "w"), "zh", 300m);
                insertSub(sub, _data.equalfilter("type", "l"), "zh", 300m);
            }

        }
        public void insertSub(List<DataTable> list, DataTable dt, string sub, decimal fullmark)
        {
            
            DataTable data = new DataTable();
            data.Columns.Add("mark", typeof(decimal));
            data.Columns.Add("difficulty", typeof(decimal));

            decimal flag = 0m;
            decimal interval = 1.0m;
            if (_fullmark > 40.0m)
            {
                interval = Math.Floor(_fullmark / 40.0m);
                flag = (interval + 1) / 2.0m;

                int j = 0;
                for (j = 0; j < 40; j++)
                {
                    DataRow inter_row = data.NewRow();
                    inter_row["mark"] = flag;
                    inter_row["difficulty"] = 0;
                    flag += interval;
                    data.Rows.Add(inter_row);
                }
                if ((_fullmark - 40.0m * interval) != 0)
                {
                    DataRow last_row = data.NewRow();
                    last_row["mark"] = 40.0m * interval + (_fullmark - 40.0m * interval + 1) / 2.0m;
                    last_row["difficulty"] = 0;
                    data.Rows.Add(last_row);
                }
            }
            else
            {
                int j = 0;
                for (j = 0; j < _fullmark; j++)
                {
                    DataRow inter_row = data.NewRow();
                    inter_row["mark"] = Convert.ToDecimal(j + 1);
                    inter_row["difficulty"] = 0;
                    data.Rows.Add(inter_row);
                }
            }

            
            var yw = from row in dt.AsEnumerable()
                     group row by row.Field<decimal>("zf") into grp
                     orderby grp.Key ascending
                     select new
                     {
                         mark = grp.Key,
                         avg = grp.Average(row => row.Field<decimal>(sub)),
                         count = grp.Count()
                     };
            int dist_num = 0;
            int[] count = new int[data.Rows.Count];
            for (int k = 0; k < data.Rows.Count; k++)
            {
                count[k] = 0;
            }
            foreach (var item in yw)
            {
                //DataRow dr = data.NewRow();
                //dr["mark"] = item.mark;
                //dr["difficulty"] = item.avg / fullmark;
                //data.Rows.Add(dr);
                if (interval == 1.0m)
                    dist_num = Convert.ToInt32(Math.Floor(item.mark));
                else
                    dist_num = Convert.ToInt32(Math.Ceiling(item.mark / interval));
                if (dist_num > 40)
                {
                    data.Rows[40]["difficulty"] = (decimal)data.Rows[40]["difficulty"] + Convert.ToDecimal(item.count * item.avg);
                    count[40] += item.count;
                }
                else if (dist_num == 0)
                {
                    data.Rows[dist_num]["difficulty"] = (decimal)data.Rows[dist_num]["difficulty"] + Convert.ToDecimal(item.count * item.avg);
                    count[dist_num] += item.count;
                }
                else
                {
                    data.Rows[dist_num - 1]["difficulty"] = (decimal)data.Rows[dist_num - 1]["difficulty"] + Convert.ToDecimal(item.count * item.avg);
                    count[dist_num - 1] += item.count;
                }
            }
            for (int i = 0; i < data.Rows.Count; i++)
            {
                if (count[i] == 0)
                    data.Rows[i]["difficulty"] = 0;
                    
                else
                    data.Rows[i]["difficulty"] = ((decimal)data.Rows[i]["difficulty"] / count[i]) / fullmark;

            }
            while (true)
            {
                if((decimal)data.Rows[data.Rows.Count - 1]["difficulty"] == 0)
                    data.Rows.Remove(data.Rows[data.Rows.Count - 1]);
                else
                    break;
            }
            //for (int i = 0; i < data.Rows.Count; i++)
            //{
            //    if ((decimal)data.Rows[i]["difficulty"] == 0)
            //        data.Rows.Remove(data.Rows[i]);
            //}
            list.Add(data);
        }
        public void single_process(DataTable dt, ZF_worddata data)
        {
            data.total_num = dt.Rows.Count;
            data.fullmark = _fullmark;
            data.max = Convert.ToDecimal(dt.Compute("Max(zf)", ""));
            data.min = Convert.ToDecimal(dt.Compute("Min(zf)", ""));
            data.avg = Convert.ToDecimal(dt.Compute("Avg(zf)", ""));
            Partition_statistic.stdev stdev = new Partition_statistic.stdev(data.total_num, data.avg);
            foreach (DataRow dr in dt.Rows)
            {
                stdev.add(Convert.ToDecimal(dr["zf"]));
            }
            data.stDev = stdev.get_value();
            data.Dfactor = data.stDev / data.avg;
            data.difficulty = data.avg / data.fullmark;

            decimal flag = 0m;
            decimal interval = 1.0m;
            if (data.fullmark > 20.0m)
            {
                interval = Math.Floor(data.fullmark / 20.0m);
                flag = (interval + 1) / 2.0m;

                int j = 0;
                for (j = 0; j < 20; j++)
                {
                    DataRow inter_row = data.dist.NewRow();
                    inter_row["mark"] = flag;
                    inter_row["rate"] = 0;
                    flag += interval;
                    data.dist.Rows.Add(inter_row);
                }
                if ((data.fullmark - 20.0m * interval) != 0)
                {
                    DataRow last_row = data.dist.NewRow();
                    last_row["mark"] = 20.0m * interval + (data.fullmark - 20.0m * interval + 1) / 2.0m;
                    last_row["rate"] = 0;
                    data.dist.Rows.Add(last_row);
                }
            }
            else
            {
                int j = 0;
                for (j = 0; j <= data.fullmark; j++)
                {
                    DataRow inter_row = data.dist.NewRow();
                    inter_row["mark"] = Convert.ToDecimal(j);
                    inter_row["num"] = 0;
                    data.dist.Rows.Add(inter_row);
                }
            }
            var temp = from row in dt.AsEnumerable()
                       group row by row.Field<decimal>("zf") into grp
                       orderby grp.Key descending
                       select new
                       {
                           totalmark = grp.Key,
                           count = grp.Count()
                       };

            bool first = true;
            int lastcount = 0;
            foreach (var item in temp)
            {
                DataRow dr = data.frequency.NewRow();
                dr["totalmark"] = item.totalmark;
                dr["frequency"] = item.count;
                dr["rate"] = item.count / Convert.ToDecimal(data.total_num) * 100m;
                if (first)
                {
                    dr["accumulateFreq"] = item.count;
                    dr["accumulateRate"] = dr["rate"];
                    lastcount = item.count;
                    first = false;
                }
                else
                {
                    dr["accumulateFreq"] = item.count + lastcount;
                    dr["accumulateRate"] = (int)dr["accumulateFreq"] / Convert.ToDecimal(data.total_num) * 100m;
                    lastcount = (int)dr["accumulateFreq"];
                }
                data.frequency.Rows.Add(dr);
            }
            int dist_num = 0;
            for (int i = data.frequency.Rows.Count - 1; i >= 0; i--)
            {
                if (interval == 1.0m)
                {
                    dist_num = Convert.ToInt32(Math.Floor((decimal)data.frequency.Rows[i]["totalmark"]));
                    data.dist.Rows[dist_num]["rate"] = (decimal)data.frequency.Rows[dist_num]["rate"] + Convert.ToDecimal(data.frequency.Rows[i]["frequency"]);
                }
                else
                {
                    dist_num = Convert.ToInt32(Math.Ceiling((decimal)data.frequency.Rows[i]["totalmark"] / interval));
                    if (dist_num > 20)
                        data.dist.Rows[20]["rate"] = (decimal)data.frequency.Rows[20]["rate"] + Convert.ToDecimal(data.frequency.Rows[i]["frequency"]);
                    else if (dist_num == 0)
                        data.dist.Rows[dist_num]["rate"] = (decimal)data.dist.Rows[dist_num]["rate"] + Convert.ToDecimal(data.frequency.Rows[i]["frequency"]);
                    else
                        data.dist.Rows[dist_num - 1]["rate"] = (decimal)data.dist.Rows[dist_num - 1]["rate"] + Convert.ToDecimal(data.frequency.Rows[i]["frequency"]);
                }
            }
            //foreach (DataRow dr in data.dist.Rows)
            //{
            //    dr["rate"] = (decimal)dr["rate"] / Convert.ToDecimal(data.total_num) * 100m;
            //}

        }
    }
}
