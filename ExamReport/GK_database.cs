using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OleDb;
using System.Text.RegularExpressions;
using System.Diagnostics;

namespace ExamReport
{
    class GK_database
    {
        public DataTable _groups;
        public int _group_num;
        public ZK_database.GroupType _gtype;
        public decimal _divider;
        public DataTable _standard_ans;
        public DataTable _basic_data;
        public DataTable _group_data;

        public DataTable zh_single_data;
        public DataTable zh_group_data;
        string filePath;
        string file;
        string path;
        string filename;
        string filext;

        OleDbConnection dbfConnection;

        public GK_database(DataTable standard_ans, DataTable groups, ZK_database.GroupType gtype, decimal divider)
        {
            _groups = groups;
            _gtype = gtype;
            _divider = divider;
            _standard_ans = standard_ans;
            _standard_ans.PrimaryKey = new DataColumn[] { _standard_ans.Columns[0] };
            _basic_data = new DataTable();
            _group_data = new DataTable();
        }

        public GK_database()
        {
        }

        public int ZH_postprocess(DataTable zh_groups, string name)
        {
            Regex number = new Regex("^[Tt]\\d");
            zh_groups.PrimaryKey = new DataColumn[] {zh_groups.Columns[0] };
            DataRow target = zh_groups.Rows.Find(name);
            string[] tz = target[1].ToString().Trim().Split(new char[2] { ',', '，' });
            List<string> tzs = new List<string>();
            group_process(tz, tzs);
            zh_single_data = new DataTable();
            zh_single_data.Columns.Add("studentid", System.Type.GetType("System.String"));
            zh_single_data.Columns.Add("schoolcode", System.Type.GetType("System.String"));
            zh_single_data.Columns.Add("totalmark", typeof(decimal));
            foreach (string temp in tzs)
            {
                if (!_basic_data.Columns.Contains("T" + temp))
                    return -1;
                zh_single_data.Columns.Add("T" + temp, typeof(decimal));
                
            }
            int multiple_choice_num = 0;
            foreach (string temp in tzs)
            {
                if (_basic_data.Columns.Contains("D" + temp))
                {
                    zh_single_data.Columns.Add("D" + temp, typeof(string));
                    multiple_choice_num++;
                }
            }
            zh_single_data.Columns.Add("Groups", typeof(string));
            zh_single_data.Columns.Add("QX", typeof(string));

            foreach (DataRow dr in _basic_data.Rows)
            {
                DataRow newrow = zh_single_data.NewRow();
                for(int i = 0; i < zh_single_data.Columns.Count; i++)
                    newrow[i] = dr[zh_single_data.Columns[i].ColumnName];
                decimal totalmark = 0;
                foreach (DataColumn dc in zh_single_data.Columns)
                {
                    if (number.IsMatch(dc.ColumnName) && Convert.ToInt32(_standard_ans.Rows.Find(dc.ColumnName.Substring(1))["fs"]) >= 0)
                        totalmark += (decimal)newrow[dc];
                }
                newrow["totalmark"] = totalmark;
                zh_single_data.Rows.Add(newrow);
            }
            List<List<string>> group_th = new List<List<string>>();
            zh_group_data = new DataTable();
            zh_group_data.Columns.Add("studentid", typeof(string));
            zh_group_data.Columns.Add("schoolcode", typeof(string));
            zh_group_data.Columns.Add("totalmark", typeof(decimal));
            foreach (DataRow dr in zh_groups.Rows)
            {

                string group_name = dr[0].ToString().Trim();
                zh_group_data.Columns.Add(group_name, typeof(decimal));
                string[] th_string = dr[1].ToString().Trim().Split(new char[2] { ',', '，' });
                
                List<string> th = new List<string>();
                group_process(th_string, th);
                group_th.Add(th);
            }
            zh_group_data.Columns.Add("Groups", typeof(string));
            zh_group_data.Columns.Add("QX", typeof(string));

            foreach (DataRow dr in _basic_data.Rows)
            {
                DataRow newrow = zh_group_data.NewRow();
                newrow["studentid"] = dr[0].ToString();
                newrow["schoolcode"] = dr[1].ToString();
                newrow["Groups"] = ((string)dr["Groups"]).Trim();
                newrow["QX"] = dr["QX"].ToString().Trim();
                newrow["totalmark"] = dr[2];

                for (int i = 0; i < zh_groups.Rows.Count; i++)
                {
                    if (i < 3)
                    {
                        decimal mark = 0;
                        foreach (string temp in group_th[i])
                        {
                            int fs = Convert.ToInt32(_standard_ans.Rows.Find(temp)["fs"]);
                            if (Math.Abs(fs) == fs)
                                mark += (decimal)dr["T" + temp];
                        }
                        newrow[i + 3] = mark;
                    }
                    else
                    {
                        decimal mark = 0;
                        foreach (string temp in group_th[i])
                        {
                            mark += (decimal)dr["T" + temp];
                        }
                        newrow[i + 3] = mark;
                    }
                }
                zh_group_data.Rows.Add(newrow);
            }
            update_standard_ans();
            return multiple_choice_num;
        }
        public void group_process(string[] tz, List<string> tzs)
        {
            string spattern = "^\\d+~\\d+$";
            foreach (string temp in tz)
            {
                if (System.Text.RegularExpressions.Regex.IsMatch(temp, spattern))
                //if(th.Contains('~'))
                {
                    string[] num = temp.Split('~');
                    int j;
                    int size = Convert.ToInt32(num[0]) < Convert.ToInt32(num[1]) ? Convert.ToInt32(num[1]) : Convert.ToInt32(num[0]);
                    int start = Convert.ToInt32(num[0]) > Convert.ToInt32(num[1]) ? Convert.ToInt32(num[1]) : Convert.ToInt32(num[0]);
                    //此处需判断size和start的边界问题
                    for (j = start; j < size + 1; j++)
                    {
                        tzs.Add(j.ToString());
                    }

                }
                else
                    tzs.Add(temp);
            }
        }

        public void ZF_data_process(string fileadd)
        {
            filePath = @fileadd;
            file = System.IO.Path.GetFileName(filePath);
            path = System.IO.Path.GetDirectoryName(filePath);
            filename = System.IO.Path.GetFileNameWithoutExtension(filePath);
            filext = System.IO.Path.GetExtension(filePath);

            string conn = @"Provider=vfpoledb;Data Source=" + path + ";Collating Sequence=machine;";

            dbfConnection = new OleDbConnection(conn);

            OleDbDataAdapter adpt = new OleDbDataAdapter("select * from " + file + " where zf<>0", dbfConnection);
            DataSet mySet = new DataSet();
            adpt.Fill(mySet);
            dbfConnection.Close();
            _basic_data = mySet.Tables[0];

            _basic_data.Columns.Add("type", typeof(string));
            Regex w_mh = new Regex(@"^1\d+");
            Regex l_mh = new Regex(@"^5\d+");
            foreach (DataRow dr in _basic_data.Rows)
            {
                if (w_mh.IsMatch((string)dr["mh"]))
                    dr["type"] = "w";
                else if (l_mh.IsMatch((string)dr["mh"]))
                    dr["type"] = "l";
                else
                    dr["type"] = "n";
            }

        }

        public string DBF_data_process(string fileadd)
        {
            Stopwatch st = new Stopwatch();
            st.Start();
            filePath = @fileadd;
            file = System.IO.Path.GetFileName(filePath);
            path = System.IO.Path.GetDirectoryName(filePath);
            filename = System.IO.Path.GetFileNameWithoutExtension(filePath);
            filext = System.IO.Path.GetExtension(filePath);

            string conn = @"Provider=vfpoledb;Data Source=" + path + ";Collating Sequence=machine;";
            Regex topic = new Regex("^[Ss]\\d+$");
            dbfConnection = new OleDbConnection(conn);

            OleDbDataAdapter adpt = new OleDbDataAdapter("select * from " + file, dbfConnection);
            DataSet mySet = new DataSet();

            adpt.Fill(mySet);
            dbfConnection.Close();
            if (mySet.Tables.Count > 1)
                return "more than 1 tables";
            DataTable dt = mySet.Tables[0];
            int count = dt.Columns.Count;
            int i;
            DataTable basic_data = new DataTable();
            basic_data.Columns.Add("studentid", System.Type.GetType("System.String"));
            basic_data.Columns.Add("schoolcode", System.Type.GetType("System.String"));
            basic_data.Columns.Add("totalmark", typeof(decimal));
            for (i = 0; i < _standard_ans.Rows.Count; i++)
                basic_data.Columns.Add("T" + ((string)_standard_ans.Rows[i]["th"]).Trim(), System.Type.GetType("System.Decimal"));
            bool first = true;

            string omrstr = dt.Columns.Contains("Omrstr") ? "Omrstr" : "Info";

            if (!dt.Columns.Contains("Zf"))
            {
                dt.Columns.Add("Zf", typeof(decimal));
                foreach (DataRow dr in dt.Rows)
                {
                    decimal zf = 0;
                    foreach (DataColumn dc in dt.Columns)
                        if (topic.IsMatch(dc.ColumnName.ToString().Trim()))
                            zf += (decimal)dr[dc];
                    dr["Zf"] = zf;
                }
            }
            foreach (DataRow dr in dt.Rows)
            {
                string an = (string)dr[omrstr];
                char[] ans = an.Trim().ToCharArray();

                if (first)
                {
                    for (i = 0; i < ans.Length; i++)
                        basic_data.Columns.Add("D" + ((string)_standard_ans.Rows[i]["th"]).Trim(), typeof(string));
                    first = false;
                    basic_data.Columns.Add("Groups", typeof(string));
                    basic_data.Columns.Add("QX", typeof(string));
                }

                DataRow newRow = basic_data.NewRow();
                newRow["studentid"] = dr["Mh"].ToString().Trim();
                newRow["schoolcode"] = dr["Schoolcode"].ToString().Trim();
                newRow["totalmark"] = (decimal)dr["Zf"];
                if (Utils.sub_iszero && (decimal)dr["Zf"] == 0)
                    continue;
                decimal obj_mark = 0;
                for (i = 0; i < ans.Length; i++)
                {
                    string temp = ((string)_standard_ans.Rows[i]["da"]).Trim();
                    string th = "T" + ((string)_standard_ans.Rows[i]["th"]).Trim();
                    if (ans[i].ToString().Equals(temp))
                    {
                        decimal val = Convert.ToDecimal(_standard_ans.Rows[i]["fs"]);
                        newRow[th] = val;
                        obj_mark += val;
                        newRow["totalmark"] = (decimal)newRow["totalmark"] + val;

                    }
                    else if(Utils.isContain(temp, ans[i].ToString()))
                    {
                        if (Convert.ToDecimal(_standard_ans.Rows[i]["fs"]) > 1)
                        {
                            decimal val = Convert.ToDecimal(_standard_ans.Rows[i]["fs"]) / 2;
                            newRow[th] = val;
                            obj_mark += val;
                            newRow["totalmark"] = (decimal)newRow["totalmark"] + val;
                        }
                        else
                            newRow[th] = 0;

                    }
                    else
                        newRow[th] = 0.0;
                    newRow["D" + ((string)_standard_ans.Rows[i]["th"]).Trim()] = ans[i].ToString();
                }
                if (Utils.obj_iszero && obj_mark == 0)
                    continue;
                if ((decimal)newRow["totalmark"] == 0)
                    continue;
                int col = 0;
                for (i = ans.Length; i < _standard_ans.Rows.Count; i++)
                {
                    if (!topic.IsMatch(dt.Columns[col].ColumnName.ToString().Trim()))
                    {
                        return "col > dt.columns.count";
                        //error!!
                    }
                    int fs = Convert.ToInt32(_standard_ans.Rows[i]["fs"]);
                    if (Math.Abs(fs) != fs)
                    {
                        int num = Math.Abs(fs);
                        decimal mark = 0;
                        for (int k = 0; k < num; k++)
                            mark += (decimal)dr[col + k];
                        newRow["T" + (string)_standard_ans.Rows[i]["th"]] = mark;

                    }
                    else
                    {
                        newRow["T" + (string)_standard_ans.Rows[i]["th"]] = (decimal)dr[col];
                        col++;
                    }
                }
                newRow["Groups"] = "";
                newRow["QX"] = dr["Qx"].ToString().Trim();
                basic_data.Rows.Add(newRow);
            }
            
            _basic_data = basic_data.Copy();
            DataView dv = basic_data.DefaultView;
            dv.Sort = "totalmark";
            _basic_data = dv.ToTable();
            int totalsize = _basic_data.Rows.Count;
            if (_gtype.Equals(ZK_database.GroupType.population))
            {
                int remainder = 0;
                int groupnum = Math.DivRem(totalsize, Convert.ToInt32(_divider), out remainder);
                _group_num = Convert.ToInt32(_divider);
                int remainderCount = 1;
                string groupstring = "";
                for (i = 0; i < _basic_data.Rows.Count; i++)
                {
                    if (i < ((groupnum + 1) * remainder))
                    {
                        if (i % (groupnum + 1) == 0)
                        {
                            groupstring = "G" + remainderCount.ToString();
                            remainderCount++;
                        }

                    }
                    else
                    {
                        if ((i - (groupnum + 1) * remainder) % groupnum == 0)
                        {
                            groupstring = "G" + remainderCount.ToString();
                            remainderCount++;
                        }
                    }
                    _basic_data.Rows[i]["Groups"] = groupstring;
                }
            }
            else
            {
                decimal baseMark = 0.0m;
                string groupstring = "G1";
                int dividerCount = 1;
                for (i = 0; i < _basic_data.Rows.Count; i++)
                {
                    if ((decimal)_basic_data.Rows[i]["totalmark"] > (baseMark + _divider))
                    {
                        dividerCount++;
                        groupstring = "G" + dividerCount.ToString();
                        baseMark = (decimal)_basic_data.Rows[i]["totalmark"];
                    }
                    _basic_data.Rows[i]["Groups"] = groupstring;
                }
                _group_num = dividerCount;


                

            }
            create_groups();
            if (Utils.saveMidData)
            {
                create_db_tables();
                create_groups_table();
            }
            return "";
        }
        public void update_standard_ans()
        {
            for (int k = 0; k < _standard_ans.Rows.Count; k++)
            {
                int fs = Convert.ToInt32(_standard_ans.Rows[k]["fs"]);
                if (Math.Abs(fs) != fs)
                {
                    int num = Math.Abs(fs);
                    decimal mark = 0;
                    for (int j = 1; j <= num; j++)
                        mark += Convert.ToDecimal(_standard_ans.Rows[k + j]["fs"]);
                    _standard_ans.Rows[k]["fs"] = mark.ToString();
                }
            }
        }
        public void create_db_tables()
        {
            #region create table insert data
            int i = 0;
            StringBuilder objectdata = new StringBuilder();
            string newTable = filename + "_full";
            objectdata.Append("CREATE TABLE `" + newTable + "` (\n");
            objectdata.Append("\t`studentid` c(10),\n");
            objectdata.Append("\t`schoolcode` c(10),\n");
            objectdata.Append("\t`totalmark` n(4,1),\n");

            for (i = 3; i < _basic_data.Columns["d1"].Ordinal; i++)
            {
                objectdata.Append("\t`" + _basic_data.Columns[i].ColumnName + "` n(4,1),\n");
            }
            for (i = _basic_data.Columns["D1"].Ordinal; i < _basic_data.Columns.Count - 2; i++)
            {
                objectdata.Append("\t`" + _basic_data.Columns[i].ColumnName + "`c(1),\n");
            }
            objectdata.Append("\t`" + _basic_data.Columns[i].ColumnName + "` c(4),\n");
            objectdata.Append("\t`" + _basic_data.Columns[i + 1].ColumnName + "` c(4));\n");

            OleDbCommand createcommand = new OleDbCommand(objectdata.ToString(), dbfConnection);

            OleDbCommand insertcommand = new OleDbCommand();
            insertcommand.Connection = dbfConnection;
            dbfConnection.Open();
            createcommand.ExecuteNonQuery();
            //form.ShowPro(40, 2);
            OleDbTransaction trans = null;
            trans = insertcommand.Connection.BeginTransaction();
            insertcommand.Transaction = trans;

            foreach (DataRow dr in _basic_data.Rows)
            {
                objectdata.Clear();
                objectdata.Append("INSERT INTO " + newTable + " VALUES ('");
                objectdata.Append(dr[0] + "','" + dr[1] + "',");

                for (i = 2; i < _basic_data.Columns["D1"].Ordinal; i++)
                {
                    objectdata.Append(dr[i] + ",");
                }
                objectdata.Append("'");
                for (i = _basic_data.Columns["D1"].Ordinal; i < _basic_data.Columns.Count - 1; i++)
                    objectdata.Append(dr[i] + "','");

                objectdata.Append(dr[_basic_data.Columns.Count - 1] + "');");
                insertcommand.CommandText = objectdata.ToString();
                insertcommand.ExecuteNonQuery();

            }


            trans.Commit();
            dbfConnection.Close();
            #endregion
        }
        public void create_groups()
        {
            #region divide the table into groups
            //StringBuilder objectdata = new StringBuilder();
            _group_data.Columns.Add("studentid", System.Type.GetType("System.String"));
            _group_data.Columns.Add("schoolcode", System.Type.GetType("System.String"));
            _group_data.Columns.Add("totalmark", System.Type.GetType("System.Decimal"));
            ArrayList tm = new ArrayList();
            string spattern = "^\\d+~\\d+$";
            for (int i = 0; i < _groups.Rows.Count; i++)
            {
                ArrayList tz = new ArrayList();
                string row_name = _groups.Rows[i][0].ToString().Trim();
                _group_data.Columns.Add(row_name, System.Type.GetType("System.Decimal"));
                string org = _groups.Rows[i][1].ToString().Trim();
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
                            tz.Add(j);
                        }

                    }
                    else
                        tz.Add(th);
                }
                tm.Add(tz);
            }
            _group_data.Columns.Add("Groups", typeof(string));
            _group_data.Columns.Add("QX", typeof(string));
            foreach (DataRow dr in _basic_data.Rows)
            {
                DataRow newRow = _group_data.NewRow();
                newRow["studentid"] = ((string)dr[0]).Trim();
                newRow["schoolcode"] = ((string)dr[1]).Trim();
                newRow["Groups"] = ((string)dr["Groups"]).Trim();
                newRow["QX"] = dr["QX"].ToString().Trim();
                newRow["totalmark"] = dr[2];
                int j;
                for (j = 0; j < _groups.Rows.Count; j++)
                {
                    decimal count_ = 0;
                    foreach (object s in (ArrayList)tm[j])
                    {
                        count_ += (decimal)dr["T" + s.ToString()];
                    }
                    newRow[j+3] = count_;
                }
                _group_data.Rows.Add(newRow);
            }
            //objectdata.Clear();
            //string group_Table = filename + "_groups";
            //objectdata.Append("CREATE TABLE `" + group_Table + "` (\n");
            //objectdata.Append("\t`studentid` c(10),\n");
            //objectdata.Append("\t`studname` c(10),\n");
            //objectdata.Append("\t`totalmark` n(4,1),\n");
            //for (i = 3; i < _group_data.Columns.Count - 1; i++)
            //{
            //    objectdata.Append("\t`" + _group_data.Columns[i].ColumnName + "` n(4,1),\n");
            //}
            //objectdata.Append("\t`" + _group_data.Columns[i].ColumnName + "` c(4));");
            //OleDbCommand group_create = new OleDbCommand(objectdata.ToString(), dbfConnection);
            //dbfConnection.Open();
            //group_create.ExecuteNonQuery();
            //OleDbCommand group_insert = new OleDbCommand();
            //group_insert.Connection = dbfConnection;
            //OleDbTransaction group_trans = null;
            //group_trans = group_insert.Connection.BeginTransaction();
            //group_insert.Transaction = group_trans;

            //foreach (DataRow dr in _group_data.Rows)
            //{
            //    objectdata.Clear();
            //    objectdata.Append("INSERT INTO " + group_Table + " VALUES ('");
            //    objectdata.Append(dr[0] + "','" + dr[1] + "',");

            //    for (i = 2; i < _group_data.Columns.Count - 1; i++)
            //    {
            //        objectdata.Append(dr[i] + ",");
            //    }
            //    objectdata.Append("'");
            //    objectdata.Append(dr[_group_data.Columns.Count - 1] + "');");
            //    group_insert.CommandText = objectdata.ToString();
            //    group_insert.ExecuteNonQuery();

            //}
            //group_trans.Commit();
            //dbfConnection.Close();
            //st.Stop();
            #endregion
        }

        public void create_groups_table()
        {
            StringBuilder objectdata = new StringBuilder();
            objectdata.Clear();
            int i = 0;
            string group_Table = filename + "_groups";
            objectdata.Append("CREATE TABLE `" + group_Table + "` (\n");
            objectdata.Append("\t`studentid` c(10),\n");
            objectdata.Append("\t`schoolcode` c(10),\n");
            objectdata.Append("\t`totalmark` n(4,1),\n");
            for (i = 3; i < _group_data.Columns.Count - 2; i++)
            {
                objectdata.Append("\t`" + _group_data.Columns[i].ColumnName + "` n(4,1),\n");
            }
            objectdata.Append("\t`" + _group_data.Columns[i].ColumnName + "` c(4),\n");
            objectdata.Append("\t`" + _group_data.Columns[i + 1].ColumnName + "` c(4));");
            OleDbCommand group_create = new OleDbCommand(objectdata.ToString(), dbfConnection);
            dbfConnection.Open();
            group_create.ExecuteNonQuery();
            OleDbCommand group_insert = new OleDbCommand();
            group_insert.Connection = dbfConnection;
            OleDbTransaction group_trans = null;
            group_trans = group_insert.Connection.BeginTransaction();
            group_insert.Transaction = group_trans;

            foreach (DataRow dr in _group_data.Rows)
            {
                objectdata.Clear();
                objectdata.Append("INSERT INTO " + group_Table + " VALUES ('");
                objectdata.Append(dr[0] + "','" + dr[1] + "',");

                for (i = 2; i < _group_data.Columns.Count - 2; i++)
                {
                    objectdata.Append(dr[i] + ",");
                }
                objectdata.Append("'");
                objectdata.Append(dr[_group_data.Columns.Count - 2] + "','" + dr[_group_data.Columns.Count - 1] + "');");
                group_insert.CommandText = objectdata.ToString();
                group_insert.ExecuteNonQuery();

            }
            group_trans.Commit();
            dbfConnection.Close();
        }



    }
}
