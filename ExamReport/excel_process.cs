using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Data;
using Microsoft.Office.Core;
using Microsoft.Office.Interop.Excel;
using System.Runtime.InteropServices;
using System.IO;



namespace ExamReport
{
    public class excel_process
    {
        public Application app;
        public Workbooks wbks;
        public _Workbook _wbk;
        public Sheets shs;
        public Worksheet sheet;
        public System.Data.DataTable dt;
        public List<ArrayList> data;

        string[] answer_colnam = { "th", "fs", "da" };
        string[] groups_colnam = { "tz", "th" };
        string[] colname;

        public Dictionary<string, List<string>> groups_group;

        public excel_process(string filepath)
        {
            app = new Application();
            wbks = app.Workbooks;
            _wbk = wbks.Add(filepath);
            groups_group = new Dictionary<string,List<string>>();

        }
        public List<ArrayList> getData()
        {
            data = new List<ArrayList>();
            shs = _wbk.Sheets;
            sheet = shs.get_Item(1);
            int iRowCount = sheet.UsedRange.Rows.Count;
            Range rang;
            int icol = 1;
            while (true)
            {
                if (((Range)sheet.Cells[1, icol]).Value2 == null)
                    break;
                ArrayList temp = new ArrayList();
                rang = (Range)sheet.Cells[1, icol];
                temp.Add(rang.Text.ToString().Trim());
                int irow = 2;
                while (true)
                {
                    if (((Range)sheet.Cells[irow, icol]).Value2 == null)
                        break;
                    rang = (Range)sheet.Cells[irow, icol];
                    temp.Add(rang.Text.ToString().Trim());
                    irow++;
                }
                data.Add(temp);
                icol++;
            }
            release();
            return data;
        }
        public void run(bool _type)
        {
            dt = new System.Data.DataTable();
            if (_type)
                colname = answer_colnam;
            else
                colname = groups_colnam;
            shs = _wbk.Sheets;
            sheet = shs.get_Item(1);
            int iRowCount = sheet.UsedRange.Rows.Count;

            DataColumn dc;
            Range range;
            string cellContent;
            int ColumnID = 0;
            range = (Range)sheet.Cells[1, 1];

            while (ColumnID < colname.Length)
            {
                dc = new DataColumn();
                dc.DataType = System.Type.GetType("System.String");
                dc.ColumnName = colname[ColumnID];
                dt.Columns.Add(dc);

                ColumnID++;
            }
            //End  
            if (_type)
            {
                for (int iRow = 1; iRow <= iRowCount; iRow++)
                {
                    if (((Range)sheet.Cells[iRow, 1]).Value2 == null)
                        break;
                    DataRow dr = dt.NewRow();

                    for (int iCol = 1; iCol <= colname.Length; iCol++)
                    {

                        range = (Range)sheet.Cells[iRow, iCol];
                        if (!_type && iCol == 1)
                        {
                            if (groups_group.ContainsKey(range.Text.ToString()))
                            {
                                List<string> temp = groups_group[range.Text.ToString()];

                            }
                            else
                            {

                            }

                        }
                        //switch (dt.Columns[iCol].ColumnName)
                        //{
                        //    case "th":
                        //        cellContent = (range.Value2 == null) ? "" : range.Text.ToString();
                        //        break;
                        //    case "fs":
                        //        cellContent = (range.Value2 == null) ? "" : range.Value;
                        //}

                        cellContent = (range.Value2 == null) ? "" : range.Text.ToString();

                        //if (iRow == 1)  
                        //{  
                        //    dt.Columns.Add(cellContent);  
                        //}  
                        //else  
                        //{  
                        dr[iCol - 1] = cellContent;
                        //}  
                        CheckCell(iCol, cellContent);
                    }

                    //if (iRow != 1)  
                    dt.Rows.Add(dr);
                }
            }
            else
            {
                for (int iRow = 1; iRow <= iRowCount; iRow++)
                {
                    var cell = sheet.Cells[iRow, 1];
                    if (cell.MergeCells == false && ((Range)sheet.Cells[iRow, 1]).Value2 == null)
                        break;
                    DataRow dr = dt.NewRow();
                    string key = GetValue(iRow, 1);
                    for (int iCol = 2; iCol <= colname.Length + 1; iCol++)
                    {

                        range = (Range)sheet.Cells[iRow, iCol];
                        if (iCol == 2)
                        {
                            if (groups_group.ContainsKey(key))
                            {
                                List<string> temp = groups_group[key];
                                temp.Add(range.Text.ToString());
                            }
                            else
                            {
                                List<string> temp = new List<string>();
                                temp.Add(range.Text.ToString());
                                groups_group.Add(key, temp);
                            }

                        }
                        //switch (dt.Columns[iCol].ColumnName)
                        //{
                        //    case "th":
                        //        cellContent = (range.Value2 == null) ? "" : range.Text.ToString();
                        //        break;
                        //    case "fs":
                        //        cellContent = (range.Value2 == null) ? "" : range.Value;
                        //}

                        cellContent = (range.Value2 == null) ? "" : range.Text.ToString();

                        //if (iRow == 1)  
                        //{  
                        //    dt.Columns.Add(cellContent);  
                        //}  
                        //else  
                        //{  
                        dr[iCol - 2] = cellContent;
                        //}  
                    }

                    //if (iRow != 1)  
                    dt.Rows.Add(dr);
                }
            }
            release();
        }
        public string GetValue(int row, int col)
        {
            // 取得单元格.
            var cell = sheet.Cells[row, col];
            if (cell.MergeCells == true)
            {
                // 本单元格是 “合并单元格”
                if (cell.MergeArea.Row == row
                    && cell.MergeArea.Column == col)
                {
                    // 当前单元格 就是 合并单元格的 左上角 内容.
                    return cell.Text;
                }
                else
                {
                    // 返回 合并单元格的 左上角 内容.
                    return sheet.Cells[cell.MergeArea.Row, cell.MergeArea.Column].Text;
                }
            }
            else
            {
                // 本单元格是 “普通单元格”
                // 获取文本信息.
                return cell.Text;
            }
        }

        public void CheckCell(int col, string value)
        {
            switch (col)
            {
                case 1:
                    break;
                case 2:
                    try
                    {
                        int mark = Convert.ToInt32(value);
                        if (Math.Abs(mark) != mark && !(Utils.subject.Contains("理综") || Utils.subject.Contains("文综")))
                            throw new FormatException();
                    }
                    catch (FormatException e)
                    {
                        release();
                        throw new ArgumentException("标准答案中满分值应该为正整数");
                    }
                    break;
                case 3:
                    string sa = Utils.choiceTransfer(value);
                    if (sa == null)
                    {
                        release();
                        throw new ArgumentException("标准答案中选择题答案”" + value + "“未定义");
                    }
                    break;
                default:
                    break;

            }
            
        }

        public void release()
        {
            _wbk.Close();
            _wbk = null;
            app.Quit();
            KillSpecialExcel();
            //System.Runtime.InteropServices.Marshal.ReleaseComObject((object)app);
            //System.Runtime.InteropServices.Marshal.ReleaseComObject((object)wbks);
            //System.Runtime.InteropServices.Marshal.ReleaseComObject((object)_wbk);
            //System.Runtime.InteropServices.Marshal.ReleaseComObject((object)shs);
            //System.Runtime.InteropServices.Marshal.ReleaseComObject((object)sheet);

            app = null;
            wbks = null;
            shs = null;
            sheet = null;
        }
        [DllImport("user32.dll", SetLastError = true)]

        static extern int GetWindowThreadProcessId(IntPtr hWnd, out int lpdwProcessId);



        //推荐这个方法，找了很久，不容易啊  

        public void KillSpecialExcel()
        {

            try
            {

                if (app != null)
                {

                    int lpdwProcessId;

                    GetWindowThreadProcessId(new IntPtr(app.Hwnd), out lpdwProcessId);



                    System.Diagnostics.Process.GetProcessById(lpdwProcessId).Kill();

                }

            }

            catch (Exception ex)
            {

                Console.WriteLine("Delete Excel Process Error:" + ex.Message);

            }

        }


    }
}
