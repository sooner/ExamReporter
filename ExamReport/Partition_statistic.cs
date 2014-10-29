using System;
using System.Collections;
using System.Linq;
using System.Text;
using System.Data;
using System.Text.RegularExpressions;
namespace ExamReport
{
    class Partition_statistic
    {
        public PartitionData result;

        public DataTable _basic_data;
        public DataTable _groups_data;

        public decimal _fullmark;
        
        public int _groupnum;

        public DataTable _standard_ans;
        public DataTable _groups_ans;



        public Partition_statistic(string title, DataTable dt, decimal fullmark, DataTable standard_ans, DataTable groups_table, DataTable groups_ans, int groupnum)
        {
            _basic_data = dt;
            _groups_data = groups_table;
            _fullmark = fullmark;
            _groupnum = groupnum;
            _standard_ans = standard_ans;
            _groups_ans = groups_ans;

            _standard_ans.PrimaryKey = new DataColumn[] { _standard_ans.Columns["th"] };
            if (Utils.WSLG)
                result = new WSLG_partitiondata(title);
            else
                result = new PartitionData(title);

        }

        public void statistic_process(bool isZonghe)
        {
            ArrayList stdevlist = new ArrayList();
            result.total_num = _basic_data.Rows.Count;
            if (Utils.WSLG)
            {
                ((WSLG_partitiondata)result).PLN = Convert.ToInt32(Math.Ceiling(_basic_data.Rows.Count * 0.27));
                ((WSLG_partitiondata)result).PHN = _basic_data.Rows.Count - ((WSLG_partitiondata)result).PLN + 1;
            }
            result.fullmark = _fullmark;
            result.max = (decimal)_basic_data.Compute("Max(totalmark)", "");
            result.min = (decimal)_basic_data.Compute("Min(totalmark)", "");
            result.avg = (decimal)_basic_data.Compute("Avg(totalmark)", "");

            stdev total_stdev = new stdev(result.total_num, result.avg);
            stdevlist.Add(total_stdev);

            result.difficulty = result.avg / result.fullmark;
            Regex number = new Regex("^[Tt]\\d+");
            if (Utils.WSLG)
                ((WSLG_partitiondata)result).total.Add(new WSLG_partitiondata.Disc(((WSLG_partitiondata)result).PLN, result.fullmark));
            foreach (DataColumn dc in _basic_data.Columns)
            {
                if (number.IsMatch(dc.ColumnName))
                {
                    DataRow dr = result.total_analysis.NewRow();
                    string topic_num = dc.ColumnName.Substring(1);
                    dr["number"] = dc.ColumnName;
                    dr["fullmark"] = Convert.ToDecimal(_standard_ans.Rows.Find(topic_num)["fs"]);
                    dr["max"] = _basic_data.Compute("Max([" + dc.ColumnName + "])", "");
                    dr["min"] = _basic_data.Compute("Min([" + dc.ColumnName + "])", "");
                    dr["avg"] = _basic_data.Compute("Avg([" + dc.ColumnName + "])", "");
                    stdev single_stdev = new stdev(result.total_num, (decimal)dr["avg"]);
                    stdevlist.Add(single_stdev);
                    dr["difficulty"] = (decimal)dr["avg"] / (decimal)dr["fullmark"];
                    if (Utils.WSLG)
                    {
                        ((WSLG_partitiondata)result).total.Add(new WSLG_partitiondata.Disc(((WSLG_partitiondata)result).PLN, (decimal)dr["number"]));
                    }
                    result.total_analysis.Rows.Add(dr);
                }
            }

            int row = 1;
            foreach (DataRow dr in _basic_data.Rows)
            {
                ((stdev)stdevlist[0]).add((decimal)dr["totalmark"]);
                if (Utils.WSLG)
                {
                    if (row <= ((WSLG_partitiondata)result).PLN)
                        ((WSLG_partitiondata)result).total[0].AddData((decimal)dr["totalmark"], true);
                    else if (row >= ((WSLG_partitiondata)result).PHN)
                        ((WSLG_partitiondata)result).total[0].AddData((decimal)dr["totalmark"], false);
                }
                int CoCount = 1;
                foreach (DataColumn dc in _basic_data.Columns)
                {
                    if (number.IsMatch(dc.ColumnName))
                    {
                        ((stdev)stdevlist[CoCount]).add((decimal)dr[dc]);
                        if (Utils.WSLG)
                        {
                            if (row <= ((WSLG_partitiondata)result).PLN)
                                ((WSLG_partitiondata)result).total[CoCount].AddData((decimal)dr[dc], true);
                            else if (row >= ((WSLG_partitiondata)result).PHN)
                                ((WSLG_partitiondata)result).total[CoCount].AddData((decimal)dr[dc], false);
                        }
                        CoCount++;
                    }
                }
                row++;
            }

            result.stDev = ((stdev)stdevlist[0]).get_value();
            result.Dfactor = result.stDev / result.avg;
            if (Utils.WSLG)
                ((WSLG_partitiondata)result).discriminant = ((WSLG_partitiondata)result).total[0].GetAns();
            int count = 1;
            foreach (DataRow dr in result.total_analysis.Rows)
            {
                dr["stDev"] = ((stdev)stdevlist[count]).get_value();
                if ((decimal)dr["avg"] == 0)
                    dr["dfactor"] = 0m;
                else
                    dr["dfactor"] = (decimal)dr["stDev"] / (decimal)dr["avg"];
                if (Utils.WSLG)
                    ((WSLG_partitiondata)result).total_discriminant.Add(((WSLG_partitiondata)result).total[count].GetAns());
                count++;
            }
            //此处groups表增加列时需要更改上限

            #region group table
            ArrayList groupStdev = new ArrayList();

            for (int i = 3; i < _groups_data.Columns.Count - 2; i++)
            {
                DataRow dr = result.groups_analysis.NewRow();
                dr["number"] = _groups_data.Columns[i].ColumnName;
                dr["fullmark"] = group_fullmark(dr["number"].ToString(), i - 3);
                dr["max"] = _groups_data.Compute("Max([" + _groups_data.Columns[i].ColumnName + "])", "");
                dr["min"] = _groups_data.Compute("Min([" + _groups_data.Columns[i].ColumnName + "])", "");
                dr["avg"] = _groups_data.Compute("Avg([" + _groups_data.Columns[i].ColumnName + "])", "");
                stdev temp = new stdev(result.total_num, (decimal)dr["avg"]);
                groupStdev.Add(temp);
                dr["difficulty"] = (decimal)dr["avg"] / (decimal)dr["fullmark"];

                if (Utils.WSLG)
                {
                    ((WSLG_partitiondata)result).group.Add(new WSLG_partitiondata.Disc(((WSLG_partitiondata)result).PLN, (decimal)dr["number"]));
                }
                result.groups_analysis.Rows.Add(dr);
            }
            //修改上限
            row = 1;
            foreach (DataRow dr in _groups_data.Rows)
            {
                for (int i = 3; i < _groups_data.Columns.Count - 2; i++)
                {
                    ((stdev)groupStdev[i - 3]).add((decimal)dr[_groups_data.Columns[i]]);
                    if (Utils.WSLG)
                    {
                        if (row <= ((WSLG_partitiondata)result).PLN)
                            ((WSLG_partitiondata)result).group[row].AddData((decimal)dr[i], true);
                        else if (row >= ((WSLG_partitiondata)result).PHN)
                            ((WSLG_partitiondata)result).group[row].AddData((decimal)dr[i], false);
                    }

                }
                row++;
            }
            count = 0;
            foreach (DataRow dr in result.groups_analysis.Rows)
            {
                dr["stDev"] = ((stdev)groupStdev[count]).get_value();
                if ((decimal)dr["avg"] == 0)
                    dr["dfactor"] = 0m;
                else
                    dr["dfactor"] = (decimal)dr["stDev"] / (decimal)dr["avg"];
                if (Utils.WSLG)
                    ((WSLG_partitiondata)result).group_discriminant.Add(((WSLG_partitiondata)result).group[count].GetAns());
                count++;
            }


            #endregion
            frequency_table();
            single_groups_analysis();
            if (!isZonghe)
            {
                single_topic_analysis();
            }
            group_mark(_basic_data);
        }
        public void single_topic_analysis()
        {
            int topic_count = 0;
            int i = 0;
            foreach (DataRow dr in result.total_analysis.Rows)
            {
                PartitionData.single_data temp = new PartitionData.single_data();
                temp.single_detail = new DataTable();
                if (!_standard_ans.Rows[topic_count]["da"].ToString().Trim().Equals(""))
                {
                    temp.single_detail.Columns.Add("mark", typeof(string));
                    for (i = 1; i <= _groupnum; i++)
                        temp.single_detail.Columns.Add("G" + i.ToString().Trim(), typeof(decimal));
                    temp.single_detail.Columns.Add("frequency", typeof(int));
                    temp.single_detail.Columns.Add("rate", typeof(decimal));

                    temp.single_detail.Columns.Add("avg", typeof(decimal));

                    temp.single_detail.PrimaryKey = new DataColumn[] { temp.single_detail.Columns["mark"] };

                    var single_avg = from row in _basic_data.AsEnumerable()
                                     group row by row.Field<string>("D" + dr["number"].ToString().Substring(1)) into grp
                                     select new
                                     {
                                         choice = grp.Key,
                                         count = grp.Count(),
                                         avg = grp.Average(row => row.Field<decimal>("totalmark"))
                                     };
                    foreach (var item in single_avg)
                    {
                        DataRow single_row = temp.single_detail.NewRow();
                        single_row["mark"] = choiceTransfer(item.choice.ToString());
                        single_row["frequency"] = item.count;
                        single_row["rate"] = item.count / Convert.ToDecimal(result.total_num) * 100;
                        single_row["avg"] = item.avg;



                        for (i = 1; i <= _groupnum; i++)
                        {
                            single_row["G" + i.ToString().Trim()] = 0m;
                        }

                        temp.single_detail.Rows.Add(single_row);

                    }



                    var groups = from row in _basic_data.AsEnumerable()
                                 group row by new
                                 {
                                     groups = row.Field<string>("Groups"),
                                     choice = row.Field<string>("D" + dr["number"].ToString().Substring(1))
                                 } into grp
                                 select new
                                 {
                                     groups = grp.Key.groups,
                                     choice = grp.Key.choice,
                                     count = grp.Count(),

                                 };
                    foreach (var item in groups)
                    {
                        DataRow groups_row = temp.single_detail.Rows.Find(choiceTransfer(item.choice.ToString()));
                        groups_row[item.groups.ToString().Trim()] = item.count;
                    }

                    var vertical = from row in _basic_data.AsEnumerable()
                                   group row by row.Field<string>("Groups") into grp
                                   select new
                                   {
                                       groups = grp.Key,
                                       count = grp.Count(),
                                       avg = grp.Average(row => row.Field<decimal>(dr["number"].ToString().Trim()))
                                   };
                    DataRow single_total_row = temp.single_detail.NewRow();
                    DataRow single_avg_row = temp.single_detail.NewRow();
                    single_total_row["mark"] = "合计";
                    single_avg_row["mark"] = "得分率";
                    for (i = 1; i <= _groupnum; i++)
                    {
                        single_total_row["G" + i.ToString().Trim()] = 0m;
                        single_avg_row["G" + i.ToString().Trim()] = 0m;
                    }
                    foreach (var item in vertical)
                    {
                        single_total_row[item.groups.ToString().Trim()] = Convert.ToDecimal(item.count);
                        single_avg_row[item.groups.ToString().Trim()] = item.avg / (decimal)dr["fullmark"];
                    }
                    single_total_row["frequency"] = result.total_num;
                    single_total_row["rate"] = 100.0m;
                    single_total_row["avg"] = 0m;

                    single_avg_row["frequency"] = 0;
                    single_avg_row["rate"] = 0m;
                    single_avg_row["avg"] = 0m;

                    temp.single_detail.Rows.Add(single_total_row);
                    temp.single_detail.Rows.Add(single_avg_row);



                    if (_standard_ans.Rows.Find(dr["number"].ToString().Substring(1))["da"].ToString().Trim().Equals("1") ||
                       _standard_ans.Rows.Find(dr["number"].ToString().Substring(1))["da"].ToString().Trim().Equals("2") ||
                       _standard_ans.Rows.Find(dr["number"].ToString().Substring(1))["da"].ToString().Trim().Equals("4") ||
                       _standard_ans.Rows.Find(dr["number"].ToString().Substring(1))["da"].ToString().Trim().Equals("8") ||
                       _standard_ans.Rows.Find(dr["number"].ToString().Substring(1))["da"].ToString().Trim().Equals("@") ||
                       _standard_ans.Rows.Find(dr["number"].ToString().Substring(1))["da"].ToString().Trim().Equals("p") ||
                       _standard_ans.Rows.Find(dr["number"].ToString().Substring(1))["da"].ToString().Trim().Equals("P"))
                    {


                        temp.stype = WordData.single_type.single;

                        DataTable _single_detail = temp.single_detail.Clone();
                        insertRow(temp.single_detail.Rows.Find("合计"), _single_detail, 0);
                        insertRow(temp.single_detail.Rows.Find("得分率"), _single_detail, 1);

                        temp.single_detail.Rows.Find("合计").Delete();
                        temp.single_detail.Rows.Find("得分率").Delete();
                        if (temp.single_detail.Rows.Contains("G"))
                        {
                            insertRow(temp.single_detail.Rows.Find("G"), _single_detail, 0);
                            temp.single_detail.Rows.Find("G").Delete();
                        }
                        if (temp.single_detail.Rows.Contains("F"))
                        {
                            insertRow(temp.single_detail.Rows.Find("F"), _single_detail, 0);
                            temp.single_detail.Rows.Find("F").Delete();
                        }
                        if (temp.single_detail.Rows.Contains("E"))
                        {
                            insertRow(temp.single_detail.Rows.Find("E"), _single_detail, 0);
                            temp.single_detail.Rows.Find("E").Delete();
                        }
                        if (temp.single_detail.Rows.Contains("D"))
                        {
                            insertRow(temp.single_detail.Rows.Find("D"), _single_detail, 0);
                            temp.single_detail.Rows.Find("D").Delete();
                        }
                        if (temp.single_detail.Rows.Contains("C"))
                        {
                            insertRow(temp.single_detail.Rows.Find("C"), _single_detail, 0);
                            temp.single_detail.Rows.Find("C").Delete();
                        }
                        if (temp.single_detail.Rows.Contains("B"))
                        {
                            insertRow(temp.single_detail.Rows.Find("B"), _single_detail, 0);
                            temp.single_detail.Rows.Find("B").Delete();
                        }
                        if (temp.single_detail.Rows.Contains("A"))
                        {
                            insertRow(temp.single_detail.Rows.Find("A"), _single_detail, 0);
                            temp.single_detail.Rows.Find("A").Delete();
                        }
                        temp.single_detail.AcceptChanges();
                        DataRow nochoice_row = _single_detail.NewRow();
                        nochoice_row["mark"] = "未选或多选";
                        for (i = 1; i <= _groupnum; i++)
                            nochoice_row["G" + i.ToString().Trim()] = 0m;
                        nochoice_row["frequency"] = 0;
                        nochoice_row["rate"] = 0m;
                        nochoice_row["avg"] = 0m;
                        foreach (DataRow temp_dr in temp.single_detail.Rows)
                        {
                            nochoice_row["avg"] = (decimal)nochoice_row["avg"] + (decimal)temp_dr["avg"] * (int)temp_dr["frequency"];
                            nochoice_row["frequency"] = (int)nochoice_row["frequency"] + (int)temp_dr["frequency"];
                            for (i = 1; i <= _groupnum; i++)
                                nochoice_row["G" + i.ToString().Trim()] = (decimal)nochoice_row["G" + i.ToString().Trim()] + (decimal)temp_dr["G" + i.ToString().Trim()];

                        }
                        nochoice_row["rate"] = (int)nochoice_row["frequency"] / Convert.ToDecimal(result.total_num) * 100m;
                        if ((int)nochoice_row["frequency"] == 0)
                            nochoice_row["avg"] = 0;
                        else
                            nochoice_row["avg"] = (decimal)nochoice_row["avg"] / (int)nochoice_row["frequency"];


                        _single_detail.Rows.InsertAt(nochoice_row, _single_detail.Rows.Count - 2);
                        temp.single_detail = _single_detail;




                    }
                    else
                    {
                        temp.stype = WordData.single_type.multiple;


                    }

                    DataRow ans_row = temp.single_detail.Rows.Find(choiceTransfer(_standard_ans.Rows.Find(dr["number"].ToString().Substring(1))["da"].ToString().Trim()));
                    ans_row["mark"] = "*" + ans_row["mark"];


                }
                else
                {
                    temp.stype = WordData.single_type.sub;
                    temp.single_detail.Columns.Add("mark", typeof(string));
                    for (i = 1; i <= _groupnum; i++)
                        temp.single_detail.Columns.Add("G" + i.ToString().Trim(), typeof(int));
                    temp.single_detail.Columns.Add("frequency", typeof(decimal));
                    temp.single_detail.Columns.Add("rate", typeof(decimal));
                    temp.single_detail.Columns.Add("avg", typeof(decimal));

                    temp.single_detail.PrimaryKey = new DataColumn[] { temp.single_detail.Columns["mark"] };

                    var single_avg = from row in _basic_data.AsEnumerable()
                                     group row by row.Field<decimal>(dr["number"].ToString().Trim()) into grp
                                     orderby grp.Key ascending
                                     select new
                                     {
                                         mark = grp.Key,
                                         count = grp.Count(),
                                         avg = grp.Average(row => row.Field<decimal>("totalmark"))
                                     };
                    foreach (var item in single_avg)
                    {
                        DataRow temp_dr = temp.single_detail.NewRow();
                        temp_dr["mark"] = Convert.ToInt32(item.mark).ToString().Trim() + "～";
                        temp_dr["frequency"] = item.count;
                        temp_dr["rate"] = item.count / Convert.ToDecimal(result.total_num) * 100;
                        temp_dr["avg"] = item.avg;
                        for (i = 1; i <= _groupnum; i++)
                        {
                            temp_dr["G" + i.ToString().Trim()] = 0m;
                        }
                        temp.single_detail.Rows.Add(temp_dr);
                    }

                    var gdata = from row in _basic_data.AsEnumerable()
                                group row by new
                                {
                                    groups = row.Field<string>("Groups"),
                                    mark = row.Field<decimal>(dr["number"].ToString().Trim())
                                } into grp
                                select new
                                {
                                    groups = grp.Key.groups,
                                    mark = grp.Key.mark,
                                    count = grp.Count()
                                };
                    foreach (var item in gdata)
                    {
                        DataRow temp_dr = temp.single_detail.Rows.Find(Convert.ToInt32(item.mark).ToString().Trim() + "～");
                        temp_dr[item.groups.ToString().Trim()] = item.count;

                    }

                    var vertical = from row in _basic_data.AsEnumerable()
                                   group row by row.Field<string>("Groups") into grp
                                   select new
                                   {
                                       groups = grp.Key,
                                       count = grp.Count(),
                                       avg = grp.Average(row => row.Field<decimal>(dr["number"].ToString().Trim()))
                                   };
                    DataRow total_dr = temp.single_detail.NewRow();
                    DataRow avg_dr = temp.single_detail.NewRow();

                    total_dr["mark"] = "合计";
                    total_dr["frequency"] = result.total_num;
                    total_dr["rate"] = 100.0m;
                    total_dr["avg"] = result.avg;

                    avg_dr["mark"] = "得分率";
                    avg_dr["frequency"] = 0;
                    avg_dr["rate"] = 0m;
                    avg_dr["avg"] = 0m;

                    for (i = 1; i <= _groupnum; i++)
                    {
                        total_dr["G" + i.ToString().Trim()] = 0m;
                        avg_dr["G" + i.ToString().Trim()] = 0m;
                    }
                    foreach (var item in vertical)
                    {
                        total_dr[item.groups.ToString().Trim()] = Convert.ToDecimal(item.count);
                        avg_dr[item.groups.ToString().Trim()] = item.avg / (decimal)dr["fullmark"];
                    }

                    temp.single_detail.Rows.Add(total_dr);
                    temp.single_detail.Rows.Add(avg_dr);




                }
                topic_count++;
                result.single_topic_analysis.Add(temp);
            }


        }
        public void insertRow(DataRow insert_row, DataTable target, int pos)
        {
            DataRow dr = target.NewRow();
            dr.ItemArray = insert_row.ItemArray;
            target.Rows.InsertAt(dr, pos);
        }
        public void single_groups_analysis()
        {
            foreach (DataRow dr in result.groups_analysis.Rows)
            {
                PartitionData.group_data data = new PartitionData.group_data();
                data.group_detail = new DataTable();
                data.group_dist = new DataTable();
                data.group_dist.Columns.Add("mark", typeof(decimal));
                data.group_dist.Columns.Add("rate", typeof(decimal));
                decimal flag = 0m;
                decimal interval = 1.0m;
                bool interval_flag = true;
                if ((decimal)dr["fullmark"] > 20.0m)
                {
                    interval = Math.Floor((decimal)dr["fullmark"] / 20.0m);
                    flag = (interval + 1) / 2.0m;

                    int j = 0;
                    for (j = 0; j < 20; j++)
                    {
                        DataRow inter_row = data.group_dist.NewRow();
                        inter_row["mark"] = flag;
                        inter_row["rate"] = 0;
                        flag += interval;
                        data.group_dist.Rows.Add(inter_row);
                    }
                    if (((decimal)dr["fullmark"] - 20.0m * interval) != 0)
                    {
                        DataRow last_row = data.group_dist.NewRow();
                        last_row["mark"] = 20.0m * interval + ((decimal)dr["fullmark"] - 20.0m * interval + 1) / 2.0m;
                        last_row["rate"] = 0;
                        data.group_dist.Rows.Add(last_row);
                    }
                    interval_flag = false;
                }
                else
                {
                    
                    int j = 0;
                    for (j = 0; j <= (decimal)dr["fullmark"]; j++)
                    {
                        DataRow inter_row = data.group_dist.NewRow();
                        inter_row["mark"] = Convert.ToDecimal(j);
                        inter_row["rate"] = 0;
                        data.group_dist.Rows.Add(inter_row);
                    }
                }

                var freq = from row in _groups_data.AsEnumerable()
                           group row by row.Field<decimal>(dr["number"].ToString().Trim()) into grp
                           orderby grp.Key ascending
                           select new
                       {
                           count = grp.Count(),
                           mark = grp.Key,
                           avg = grp.Average(row => row.Field<decimal>("totalmark"))
                       };
                data.group_detail.Columns.Add("mark", typeof(string));
                for (int i = 1; i <= _groupnum; i++)
                    data.group_detail.Columns.Add("G" + i.ToString(), typeof(decimal));
                data.group_detail.Columns.Add("frequency", typeof(int));
                data.group_detail.Columns.Add("rate", typeof(decimal));
                data.group_detail.Columns.Add("avg", typeof(decimal));
                data.group_detail.PrimaryKey = new DataColumn[] { data.group_detail.Columns["mark"] };

                int dist_num = 0;
                foreach (var item in freq)
                {
                    DataRow newrow = data.group_detail.NewRow();
                    newrow["mark"] = Convert.ToInt32(item.mark).ToString().Trim() + "～";
                    newrow["frequency"] = item.count;
                    newrow["rate"] = item.count / Convert.ToDecimal(result.total_num) * 100;
                    newrow["avg"] = item.avg;
                    for (int i = 1; i <= _groupnum; i++)
                        newrow["G" + i.ToString()] = 0.0m;
                    data.group_detail.Rows.Add(newrow);

                    if (interval_flag)
                    {
                        dist_num = Convert.ToInt32(Math.Floor(item.mark));
                        data.group_dist.Rows[dist_num]["rate"] = (decimal)data.group_dist.Rows[dist_num]["rate"] + Convert.ToDecimal(item.count);
                    }
                    else
                    {
                        dist_num = Convert.ToInt32(Math.Ceiling(item.mark / interval));
                        if (dist_num > 20)
                            data.group_dist.Rows[20]["rate"] = (decimal)data.group_dist.Rows[20]["rate"] + Convert.ToDecimal(item.count);
                        else if (dist_num == 0)
                            data.group_dist.Rows[dist_num]["rate"] = (decimal)data.group_dist.Rows[dist_num]["rate"] + Convert.ToDecimal(item.count);
                        else
                            data.group_dist.Rows[dist_num - 1]["rate"] = (decimal)data.group_dist.Rows[dist_num - 1]["rate"] + Convert.ToDecimal(item.count);

                    }
                }

                foreach (DataRow dr2 in data.group_dist.Rows)
                {
                    dr2["rate"] = (decimal)dr2["rate"] / Convert.ToDecimal(result.total_num) * 100;
                }

                var groups = from row in _groups_data.AsEnumerable()
                             group row by new
                             {
                                 groups = row.Field<string>("Groups"),
                                 mark = row.Field<decimal>(dr["number"].ToString().Trim())
                             } into grp
                             select new
                             {
                                 groups = grp.Key.groups,
                                 mark = grp.Key.mark,
                                 count = grp.Count()
                             };
                foreach (var item in groups)
                {
                    DataRow target = data.group_detail.Rows.Find(Convert.ToInt32(item.mark).ToString().Trim() + "～");
                    target[item.groups.ToString().Trim()] = item.count;
                }

                var gdata = from row in _groups_data.AsEnumerable()
                            group row by row.Field<string>("Groups") into grp
                            select new
                            {
                                gtype = grp.Key,
                                count = grp.Count(),
                                avg = grp.Average(row => row.Field<decimal>(dr["number"].ToString()))
                            };
                DataRow total = data.group_detail.NewRow();
                DataRow avg = data.group_detail.NewRow();
                for (int i = 1; i <= _groupnum; i++)
                {
                    total["G" + i.ToString()] = 0.0m;
                    avg["G" + i.ToString()] = 0.0m;
                }
                foreach (var item in gdata)
                {
                    total[item.gtype.ToString().Trim()] = item.count;
                    avg[item.gtype.ToString().Trim()] = item.avg / (decimal)dr["fullmark"];
                }
                total["mark"] = "合计";
                total["frequency"] = result.total_num;
                total["rate"] = 100.0m;
                total["avg"] = result.avg;

                avg["mark"] = "得分率";
                avg["frequency"] = 0;
                avg["rate"] = 0.0m;
                avg["avg"] = 0.0m;

                data.group_detail.Rows.Add(total);
                data.group_detail.Rows.Add(avg);

                result.single_group_analysis.Add(data);
            }
        }
        public void frequency_table()
        {
            result.total_dist.Columns.Add("mark", typeof(decimal));
            result.total_dist.Columns.Add("rate", typeof(decimal));
            decimal flag = 0m;
            decimal interval = 1.0m;
            if (result.fullmark > 20.0m)
            {
                interval = Math.Floor(result.fullmark / 20.0m);
                flag = (interval + 1) / 2.0m;

                int j = 0;
                for (j = 0; j < 20; j++)
                {
                    DataRow inter_row = result.total_dist.NewRow();
                    inter_row["mark"] = flag;
                    inter_row["rate"] = 0;
                    flag += interval;
                    result.total_dist.Rows.Add(inter_row);
                }
                if ((result.fullmark - 20.0m * interval) != 0)
                {
                    DataRow last_row = result.total_dist.NewRow();
                    last_row["mark"] = 20.0m * interval + (result.fullmark - 20.0m * interval + 1) / 2.0m;
                    last_row["rate"] = 0;
                    result.total_dist.Rows.Add(last_row);
                }
            }
            else
            {
                int j = 0;
                for (j = 0; j <= result.fullmark; j++)
                {
                    DataRow inter_row = result.total_dist.NewRow();
                    inter_row["mark"] = Convert.ToDecimal(j);
                    inter_row["num"] = 0;
                    result.total_dist.Rows.Add(inter_row);
                }
            }
            var freq = from row in _basic_data.AsEnumerable()
                       group row by row.Field<decimal>("totalmark") into grp
                       orderby grp.Key descending
                       select new
                       {
                           count = grp.Count(),
                           totalmark = grp.Key
                       };
            bool first = true;
            int last_freq = 0;
            foreach (var item in freq)
            {
                DataRow dr = result.freq_analysis.NewRow();
                dr["totalmark"] = item.totalmark;
                dr["frequency"] = item.count;
                dr["rate"] = item.count / Convert.ToDecimal(result.total_num) * 100;
                if (first)
                {
                    dr["accumulateFreq"] = dr["frequency"];
                    dr["accumulateRate"] = dr["rate"];
                    last_freq = (int)dr["frequency"];
                    first = false;
                }
                else
                {
                    dr["accumulateFreq"] = last_freq + (int)dr["frequency"];
                    dr["accumulateRate"] = Convert.ToDecimal(dr["accumulateFreq"]) / result.total_num * 100;
                    last_freq = (int)dr["accumulateFreq"];
                }
                result.freq_analysis.Rows.Add(dr);
            }
            int dist_num = 0;
            for (int i = result.freq_analysis.Rows.Count - 1; i >= 0; i--)
            {
                if (interval == 1.0m)
                {
                    dist_num = Convert.ToInt32(Math.Floor((decimal)result.freq_analysis.Rows[i]["totalmark"]));
                    result.total_dist.Rows[dist_num]["rate"] = (decimal)result.total_dist.Rows[dist_num]["rate"] + Convert.ToDecimal(result.freq_analysis.Rows[i]["frequency"]);
                }
                else
                {
                    dist_num = Convert.ToInt32(Math.Ceiling((decimal)result.freq_analysis.Rows[i]["totalmark"] / interval));
                    if (dist_num > 20)
                        result.total_dist.Rows[20]["rate"] = (decimal)result.total_dist.Rows[20]["rate"] + Convert.ToDecimal(result.freq_analysis.Rows[i]["frequency"]);
                    else if (dist_num == 0)
                        result.total_dist.Rows[dist_num]["rate"] = (decimal)result.total_dist.Rows[dist_num]["rate"] + Convert.ToDecimal(result.freq_analysis.Rows[i]["frequency"]);
                    else
                        result.total_dist.Rows[dist_num - 1]["rate"] = (decimal)result.total_dist.Rows[dist_num - 1]["rate"] + Convert.ToDecimal(result.freq_analysis.Rows[i]["frequency"]);
                }
            }
            //foreach (DataRow dr in result.total_dist.Rows)
            //{
            //    dr["rate"] = (decimal)dr["rate"] / Convert.ToDecimal(result.total_num);
            //}
        }
        public class stdev
        {
            int _total_num;
            decimal _avg;
            decimal temp;
            public stdev(int total_num, decimal avg)
            {
                _total_num = total_num;
                _avg = avg;
                temp = 0.0m;
            }
            public void add(decimal mark)
            {
                temp += (mark - _avg) * (mark - _avg);
            }

            public decimal get_value()
            {
                return Convert.ToDecimal(Math.Sqrt(Convert.ToDouble(temp / _total_num)));
            }

        }

        public decimal group_fullmark(string name, int row)
        {
            decimal fullmark = 0.0m;
            if (name.Equals("生物") || name.Equals("政治"))
            {
                fullmark = Utils.shengwu_zhengzhi;
            }
            else if (name.Equals("物理") || name.Equals("历史"))
            {
                fullmark = Utils.wuli_lishi;
            }
            else if (name.Equals("化学") || name.Equals("地理"))
            {
                fullmark = Utils.huaxue_dili;
            }
            else
            {
                string spattern = "^\\d+~\\d+$";
                string org = _groups_ans.Rows[row][1].ToString().Trim();
                string[] org_char = org.Split(new char[2] { ',', '，' });

                foreach (string th in org_char)
                {

                    if (System.Text.RegularExpressions.Regex.IsMatch(th, spattern))
                    //if(th.Contains('~'))
                    {
                        string[] num = th.Split('~');
                        int j;
                        int size = Convert.ToInt32(num[0]) < Convert.ToInt32(num[1]) ? Convert.ToInt32(num[1]) : Convert.ToInt32(num[0]);
                        int start = Convert.ToInt32(num[0]) > Convert.ToInt32(num[1]) ? Convert.ToInt32(num[1]) : Convert.ToInt32(num[0]);
                        //此处需判断size和start的边界问题
                        for (j = start; j < size + 1; j++)
                        {
                            DataRow dr = result.total_analysis.Rows.Find("t" + j.ToString());
                            fullmark += (decimal)dr["fullmark"];
                        }

                    }
                    else
                    {
                        DataRow dr = result.total_analysis.Rows.Find("t" + th.Trim());
                        fullmark += (decimal)dr["fullmark"];
                    }


                }
            }
            return fullmark;
        }
        public string choiceTransfer(string choice)
        {
            switch (choice.Trim())
            {
                case "0":
                    return "未选";
                case "1":
                    return "A";
                case "2":
                    return "B";
                case "4":
                    return "C";
                case "8":
                    return "D";
                case "@":
                    return "E";
                case "P":
                    return "F";
                case "p":
                    return "G";
                case "3":
                    return "AB";
                case "5":
                    return "AC";
                case "6":
                    return "BC";
                case "7":
                    return "ABC";
                case "9":
                    return "AD";
                case ":":
                    return "BD";
                case ";":
                    return "ABD";
                case "<":
                    return "CD";
                case "=":
                    return "ACD";
                case ">":
                    return "BCD";
                case "?":
                    return "ABCD";
                case "A":
                    return "AE";
                case "B":
                    return "BE";
                case "C":
                    return "ABE";
                case "D":
                    return "CE";
                case "E":
                    return "ACE";
                case "F":
                    return "BCE";
                case "G":
                    return "ABCE";
                case "H":
                    return "DE";
                case "I":
                    return "ADE";
                case "J":
                    return "BDE";
                case "K":
                    return "ABDE";
                case "L":
                    return "CDE";
                default:
                    return "未选";

            }

        }
        public void group_mark(DataTable dt)
        {
            var mark = from row in dt.AsEnumerable()
                       group row by row.Field<string>("Groups") into grp
                       select new
                       {
                           name = grp.Key,
                           max = grp.Max(row => row.Field<decimal>("totalmark"))
                       };
            foreach (var temp in mark)
            {
                Utils.GroupMark.Add(temp.max);
            }
        }
    }
}
