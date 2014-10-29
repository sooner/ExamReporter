using System;
using System.Collections;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OleDb;
using System.Text.RegularExpressions;
using System.Diagnostics;

namespace ExamReport
{
    class HK_database
    {
        public DataTable _groups;
        public int _group_num;
        public ZK_database.GroupType _gtype;
        public decimal _divider;
        public DataTable _standard_ans;
        public DataTable _basic_data;
        public DataTable _group_data;

        string filePath;
        string file;
        string path;
        string filename;
        string filext;

        OleDbConnection dbfConnection;

        public HK_database(DataTable standard_ans, DataTable groups, ZK_database.GroupType gtype, decimal divider)
        {
            _groups = groups;
            _gtype = gtype;
            _divider = divider;
            _standard_ans = standard_ans;
            _standard_ans.PrimaryKey = new DataColumn[] { _standard_ans.Columns["th"] };
            _basic_data = new DataTable();
            _group_data = new DataTable();
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
            Regex topic = new Regex("^[Tt]\\d+$");
            dbfConnection = new OleDbConnection(conn);


            OleDbDataAdapter adpt = new OleDbDataAdapter("select * from " + file + " where Qk<>'Q'", dbfConnection);
            DataSet mySet = new DataSet();
            try
            {
                adpt.Fill(mySet);
            }
            catch (OleDbException e)
            {
                throw new Exception("数据库文件被占用，请关闭！");
            }
            dbfConnection.Close();
            //form.ShowPro(15, 2);
            if (mySet.Tables.Count > 1)
                return "more than 1 tables";
            DataTable dt = mySet.Tables[0];
            int count = dt.Columns.Count;
            int i;
            DataTable basic_data = new DataTable();
            basic_data.Columns.Add("studentid", System.Type.GetType("System.String"));
            basic_data.Columns.Add("schoolcode", System.Type.GetType("System.String"));
            basic_data.Columns.Add("totalmark", typeof(decimal));
            //for (i = 0; i < _standard_ans.Rows.Count; i++)
            //    basic_data.Columns.Add("T" + ((string)_standard_ans.Rows[i]["th"]).Trim(), System.Type.GetType("System.Decimal"));
            //for (i = 1; i <= 66; i++)
            //    basic_data.Columns.Add("T" + i.ToString().Trim(), typeof(decimal));
            bool first = true;

            string omrstr = dt.Columns.Contains("Ttxx") ? "Ttxx" : "Info";
            foreach (DataRow dr in dt.Rows)
            {
                string an = (string)dr[omrstr];
                char[] ans = an.Trim().ToCharArray();

                string mark_string = dr["xf"].ToString().Trim();

                string[] single_mark = check_remark(mark_string.ToCharArray()).Split(' ');

                if (single_mark.Length != _standard_ans.Rows.Count)
                    throw new ArgumentException("标准答案题目数量和数据文件题目数量不一致");

                if (first)
                {
                
                    for (i = 0; i < single_mark.Length; i++)
                    {
                        try
                        {
                            basic_data.Columns.Add("T" + _standard_ans.Rows[i]["th"].ToString().Trim(), typeof(decimal));
                        }
                        catch (DuplicateNameException e)
                        {
                            throw new System.ArgumentException("标准答案题号“" + _standard_ans.Rows[i]["th"].ToString().Trim() + "”重复");
                        }
                    }
                    int single_count = 0;
                    for (i = 0; i < single_mark.Length; i++)
                    {
                        if (!_standard_ans.Rows[i]["da"].ToString().Trim().Equals(""))
                        {
                            basic_data.Columns.Add("D" + _standard_ans.Rows[i]["th"].ToString().Trim(), typeof(string));
                            single_count++;
                        }
                    }
                    if (single_count != ans.Length)
                        throw new ArgumentException("选择题答案与数据库文件答案数量不一致");
                    first = false;
                    basic_data.Columns.Add("Groups", typeof(string));
                    basic_data.Columns.Add("QX", typeof(string));
                }

                DataRow newRow = basic_data.NewRow();
                newRow["studentid"] = dr["bmh"].ToString().Trim();
                newRow["schoolcode"] = dr["kch"].ToString().Trim();
                newRow["totalmark"] = 0m;
                decimal obj_mark = 0;
                decimal sub_mark = 0;
                bool stop = false;
                
                for (i = 0; i < single_mark.Length; i++)
                {
                
                    decimal val = Convert.ToDecimal(single_mark[i]);

                    if (i < ans.Length)
                        obj_mark += val;
                    else if (i == ans.Length)
                    {
                        if (Utils.obj_iszero && obj_mark == 0)
                        {
                            stop = true;
                            break;
                        }
                        sub_mark += val;
                    }
                    else
                    {
                        sub_mark += val;
                    }
                    if (val > Convert.ToDecimal(_standard_ans.Rows[i]["fs"]))
                        throw new ArgumentException("标准答案" + _standard_ans.Rows[i]["th"].ToString() + "题总分错误,存在学生成绩大于该分数的情况");

                    newRow["totalmark"] = (decimal)newRow["totalmark"] + val;
                    newRow["T" + _standard_ans.Rows[i]["th"].ToString().Trim()] = val;
                    
                }
                if ((decimal)newRow["totalmark"] > Utils.fullmark)
                    throw new ArgumentException("科目总分设置错误，存在学生满分大于总分的情况");
                if (stop)
                    continue;
                if (Utils.sub_iszero && sub_mark == 0)
                    continue;
                int colnum = basic_data.Columns["T" + _standard_ans.Rows[single_mark.Length - 1]["th"].ToString().Trim()].Ordinal + 1;
                for (i = 0; i < ans.Length; i++)
                {

                    newRow[colnum+i] = ans[i].ToString();
                }

                
                newRow["Groups"] = "";
                newRow["QX"] = dr["Qxdm"].ToString().Trim();
                basic_data.Rows.Add(newRow);
            }
            _basic_data = basic_data.Copy();
            DataView dv = basic_data.DefaultView;
            dv.Sort = "totalmark";
            _basic_data = dv.ToTable();
            //form.ShowPro(30, 2);
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
                create_groups_file();
            }

            return "";
        }
        public string check_remark(char[] remark)
        {
            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < remark.Length; i++)
            {
                if (i > 3 && (i-4) % 5 == 0 && !remark[i].Equals(' '))
                {
                    sb.Append(" ");
                }
                sb.Append(remark[i]);
            }
            return sb.ToString();
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
                try
                {
                    _group_data.Columns.Add(row_name, System.Type.GetType("System.Decimal"));
                }
                catch (DuplicateNameException e)
                {
                    throw new System.ArgumentException("分组名“" + row_name + "”重复");
                }
                string org = _groups.Rows[i][1].ToString().Trim();
                string[] org_char = org.Split(new char[3] { ',', '，','、'});
                foreach (string th in org_char)
                {

                    if (System.Text.RegularExpressions.Regex.IsMatch(th, spattern))
                    {
                        string[] num = th.Split('~');
                        int j;
                        int size = Convert.ToInt32(num[0]) < Convert.ToInt32(num[1]) ? Convert.ToInt32(num[1]) : Convert.ToInt32(num[0]);
                        int start = Convert.ToInt32(num[0]) > Convert.ToInt32(num[1]) ? Convert.ToInt32(num[1]) : Convert.ToInt32(num[0]);
                        if (Math.Abs(size) != size || Math.Abs(start) != start)
                            throw new ArgumentException("题组“" + row_name + "”的题号" + th + "错误: " + "题号不能为负");
                        for (j = start; j < size + 1; j++)
                        {
                            if (!_standard_ans.Rows.Contains(j.ToString()))
                                throw new ArgumentException("题组“" + row_name + "”的题号" + th + "错误: " + "该题号标准答案中不存在");
                            tz.Add(j.ToString());

                        }

                    }
                    else if (_standard_ans.Rows.Contains(th))
                        tz.Add(th);
                    else
                    {
                        if(th.Equals(""))
                            throw new ArgumentException("题组“" + row_name + "”的题号错误: " + "结尾多一逗号");
                        else
                            throw new ArgumentException("题组“" + row_name + "”的题号" + th + "错误: " + "该题号标准答案中不存在");
                    }
                }
                tm.Add(tz);
            }
            _group_data.Columns.Add("Groups", typeof(string));
            _group_data.Columns.Add("Qx", typeof(string));
            foreach (DataRow dr in _basic_data.Rows)
            {
                DataRow newRow = _group_data.NewRow();
                newRow["studentid"] = ((string)dr[0]).Trim();
                newRow["schoolcode"] = ((string)dr[1]).Trim();
                newRow["Groups"] = ((string)dr["Groups"]).Trim();
                newRow["Qx"] = dr["Qx"].ToString().Trim();
                newRow["totalmark"] = dr[2];
                int j;
                for (j = 0; j < _groups.Rows.Count; j++)
                {
                    decimal count_ = 0;
                    foreach (object s in (ArrayList)tm[j])
                    {
                        count_ += (decimal)dr["T" + s.ToString()];
                    }
                    newRow[j + 3] = count_;
                }
                _group_data.Rows.Add(newRow);
            }

            //st.Stop();
            #endregion
        }
        public void create_groups_file()
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
