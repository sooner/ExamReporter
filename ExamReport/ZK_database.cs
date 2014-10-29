using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OleDb;
using System.Diagnostics;
using System.Collections;

namespace ExamReport
{
    public partial class ZK_database
    {
        public enum GroupType { population, totalmark };
        GroupType _gtype;
        decimal _divider;
        public OleDbConnection sqlConnection;
        public DataTable _basic_data;
        public DataTable _standard_ans;
        public DataTable _groups;
        public DataTable _group_data;
        public int _group_num;
        public ZK_database(DataTable standard_ans, DataTable groups, GroupType gtype, decimal divider)
        {
            _groups = groups;
            _gtype = gtype;
            _divider = divider;
            _standard_ans = standard_ans;
            _basic_data = new DataTable();
            _group_data = new DataTable();
        }
        public string DBF_data_process(string fileadd, Form1 form)
        {
            Stopwatch st = new Stopwatch();
            st.Start();
            string filePath = @fileadd;
            string file = System.IO.Path.GetFileName(filePath);
            string path = System.IO.Path.GetDirectoryName(filePath);
            string filename = System.IO.Path.GetFileNameWithoutExtension(filePath);
            string filext = System.IO.Path.GetExtension(filePath);

            string conn = @"Provider=vfpoledb;Data Source=" + path + ";Collating Sequence=machine;";
            using (OleDbConnection dbfConnection = new OleDbConnection(conn))
            {
                OleDbDataAdapter adpt = new OleDbDataAdapter("select * from " + file + " where totalmark<>0", dbfConnection);
                DataSet mySet = new DataSet();

                adpt.Fill(mySet);
                dbfConnection.Close();
                form.ShowPro(15, 2);
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
                foreach (DataRow dr in dt.Rows)
                {
                    string an = (string)dr[omrstr];
                    char[] ans = an.Trim().ToCharArray();

                    if (first)
                    {
                        for (i = 0; i < ans.Length; i++)
                        {
                            if(!_standard_ans.Rows[i]["da"].ToString().Trim().Equals(""))
                                basic_data.Columns.Add("D" + _standard_ans.Rows[i]["th"].ToString().Trim(), typeof(string));
                        }
                        first = false;
                        basic_data.Columns.Add("Groups",typeof(string));
                        basic_data.Columns.Add("QX", typeof(string));
                    }

                    DataRow newRow = basic_data.NewRow();
                    newRow["studentid"] = ((string)dr[0]).Trim();
                    newRow["schoolcode"] = dr["xxdm"].ToString().Trim();
                    newRow["totalmark"] = (decimal)dr[2];
                    decimal obj_mark = 0;
                    for (i = 0; i < ans.Length; i++)
                    {
                        
                        string temp = ((string)_standard_ans.Rows[i]["da"]).Trim();
                        string th = "T" + ((string)_standard_ans.Rows[i]["th"]).Trim();
                        if (ans[i].ToString().Equals(temp))
                        {
                            newRow[th] = Convert.ToDecimal(_standard_ans.Rows[i]["fs"]);
                            obj_mark += Convert.ToDecimal(_standard_ans.Rows[i]["fs"]);
                        }
                        else if (Utils.isContain(temp, ans[i].ToString()))
                        {
                            if (Convert.ToDecimal(_standard_ans.Rows[i]["fs"]) > 1)
                            {
                                newRow[th] = Convert.ToDecimal(_standard_ans.Rows[i]["fs"]) / 2;
                                obj_mark += (Convert.ToDecimal(_standard_ans.Rows[i]["fs"]) / 2);
                            }
                            else
                                newRow[th] = 0;
                        }
                        else
                        {
                            newRow[th] = 0.0;
                        }
                        newRow["D" + ((string)_standard_ans.Rows[i]["th"]).Trim()] = ans[i].ToString();
                    }
                    if (Utils.obj_iszero && obj_mark == 0)
                        continue;
                    int col = 3;
                    decimal sub_mark = 0;
                    for (i = ans.Length; i < _standard_ans.Rows.Count; i++)
                    {
                        if (col > dt.Columns.Count)
                        {
                            return "col > dt.columns.count";
                            //error!!
                        }
                        newRow["T" + (string)_standard_ans.Rows[i]["th"]] = (decimal)dr[col];
                        sub_mark += (decimal)dr[col];
                        col++;
                    }
                    if (Utils.sub_iszero && sub_mark == 0)
                        continue;
                    newRow["Groups"] = "";
                    newRow["QX"] = dr["qxdm"].ToString().Trim();
                    basic_data.Rows.Add(newRow);
                }
                _basic_data = basic_data.Copy();
                DataView dv = basic_data.DefaultView;
                dv.Sort = "totalmark";
                _basic_data = dv.ToTable();
                form.ShowPro(30, 2);
                int totalsize = _basic_data.Rows.Count;
                if (_gtype.Equals(GroupType.population))
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
                if (Utils.saveMidData)
                {
                    #region create table insert data
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
                    form.ShowPro(40, 2);
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
                #region divide the table into groups
                //StringBuilder objectdata = new StringBuilder();
                _group_data.Columns.Add("studentid", System.Type.GetType("System.String"));
                _group_data.Columns.Add("schoolcode", System.Type.GetType("System.String"));
                _group_data.Columns.Add("totalmark", System.Type.GetType("System.Decimal"));
                ArrayList tm = new ArrayList();
                string spattern = "^\\d+~\\d+$";
                for(i=0; i<_groups.Rows.Count; i++)
                {
                    ArrayList tz = new ArrayList();
                    string row_name = _groups.Rows[i][0].ToString().Trim();
                    _group_data.Columns.Add(row_name, System.Type.GetType("System.Decimal"));
                    string org = _groups.Rows[i][1].ToString().Trim();
                    string[] org_char = org.Split(new char[2]{',','，'});
                    foreach (string th in org_char)
                    {

                        if (System.Text.RegularExpressions.Regex.IsMatch(th, spattern))
                        //if(th.Contains('~'))
                        {
                            string[] num = th.Split('~');
                            int j;
                            int size = Convert.ToInt32(num[0]) < Convert.ToInt32(num[1])? Convert.ToInt32(num[1]): Convert.ToInt32(num[0]);
                            int start = Convert.ToInt32(num[0]) > Convert.ToInt32(num[1]) ? Convert.ToInt32(num[1]) : Convert.ToInt32(num[0]);
                            //此处需判断size和start的边界问题
                            for (j = start; j < size + 1; j++)
                            {
                                tz.Add(j);
                            }

                        }
                        else
                            tz.Add(Convert.ToInt32(th));
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
                if (Utils.saveMidData)
                {
                    StringBuilder objectdata = new StringBuilder(); ;
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
                st.Stop();
                #endregion
                return st.ElapsedMilliseconds.ToString();
            } 
            
        }
       
    }
}
