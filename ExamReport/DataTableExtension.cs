using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace ExamReport
{
    public static class DataTableExtension
    {
        public static int SeperateGroups(this DataTable dt, ZK_database.GroupType gtype, decimal divider)
        {
            int _group_num = 0;
            int totalsize = dt.Rows.Count;
            if (gtype.Equals(ZK_database.GroupType.population))
            {
                int remainder = 0;
                int groupnum = Math.DivRem(totalsize, Convert.ToInt32(divider), out remainder);
                _group_num = Convert.ToInt32(divider);
                int remainderCount = 1;
                string groupstring = "";
                for (int i = 0; i < dt.Rows.Count; i++)
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
                    dt.Rows[i]["Groups"] = groupstring;
                }
            }
            else
            {
                decimal baseMark = 0.0m;
                string groupstring = "G1";
                int dividerCount = 1;
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if ((decimal)dt.Rows[i]["totalmark"] > (baseMark + divider))
                    {
                        dividerCount++;
                        groupstring = "G" + dividerCount.ToString();
                        baseMark = (decimal)dt.Rows[i]["totalmark"];
                    }
                    dt.Rows[i]["Groups"] = groupstring;
                }
                _group_num = dividerCount;
            }
            return _group_num;
        }
        public static DataTable filteredtable(this DataTable dt, string keyword, string[] items)
        {
            DataView dv = dt.DefaultView;
            StringBuilder sb = new StringBuilder();
            sb.Clear();
            sb.Append(keyword + " IN (");
            int i;
            for(i = 0; i < items.Length - 1; i++)
            {
                sb.Append("'" + items[i] + "',");
            }
            sb.Append("'" + items[i] + "')");
            dv.RowFilter = sb.ToString();
            if (dt.Columns.Contains("totalmark"))
                dv.Sort = "totalmark";
            else if (dt.Columns.Contains("zf"))
                dv.Sort = "zf";
            return dv.ToTable();
        }

        public static DataTable equalfilter(this DataTable dt, string key, string item)
        {
            DataView dv = dt.DefaultView;
            dv.RowFilter = key + " = '" + item + "'";
            if (dt.Columns.Contains("totalmark"))
                dv.Sort = "totalmark";
            else if (dt.Columns.Contains("zf"))
                dv.Sort = "zf";
            return dv.ToTable();
        }

        public static DataTable Likefilter(this DataTable dt, string col, string key)
        {
            DataView dv = dt.DefaultView;
            dv.RowFilter = col + " like " + key;
            if (dt.Columns.Contains("totalmark"))
                dv.Sort = "totalmark";
            else if (dt.Columns.Contains("zf"))
                dv.Sort = "zf";
            return dv.ToTable();
        }

        public static decimal CalCor(this DataTable dt, string Xname, string Yname)
        {
            DataColumn Xdc = dt.Columns[Xname];
            DataColumn Ydc = dt.Columns[Yname];

            decimal Xtotal = 0;
            decimal Ytotal = 0;

            decimal XSquare = 0;
            decimal YSquare = 0;

            decimal XYSqure = 0;
            foreach (DataRow dr in dt.Rows)
            {
                decimal X = (decimal)dr[Xdc];
                decimal Y = (decimal)dr[Ydc];

                Xtotal += X;
                Ytotal += Y;

                XSquare += X * X;
                YSquare += Y * Y;

                XYSqure += X * Y;
            }

            int totalnum = dt.Rows.Count;

            decimal numerator = XYSqure - Xtotal * Ytotal / totalnum;
            decimal Xdenominator = XSquare - Xtotal * Xtotal / totalnum;
            decimal Ydenominator = YSquare - Ytotal * Ytotal / totalnum;

            return numerator / Convert.ToDecimal(Math.Sqrt(Convert.ToDouble(Xdenominator * Ydenominator)));
        }
    }
}
