using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Word = Microsoft.Office.Interop.Word;
using Excel = Microsoft.Office.Interop.Excel;
using Graph = Microsoft.Office.Interop.Graph;
using System.Drawing;
using System.Reflection;
using System.Runtime.InteropServices;

namespace ExamReport
{
    class Partition_wordcreator
    {
        private object CaptionTitle = "CaptionTitle";
        private object ExamTitle0 = "ExamTitle0";
        private object ExamTitle1 = "ExamTitle1";
        private object ExamTitle2 = "ExamTitle2";
        private object ExamTitle3 = "ExamTitle3";
        private object ExamBodyText = "ExamBodyText";
        private object TableContent = "TableContent";
        private object TableContent2 = "TableContent2";

        object oMissing = System.Reflection.Missing.Value;
        object oEndOfDoc = "\\endofdoc"; /* \endofdoc is a predefined bookmark */
        Word._Application oWord;
        Word._Document oDoc;
        ArrayList _sdata;
        object oParagrahbreak = Microsoft.Office.Interop.Word.WdBreakType.wdLineBreak;
        Object oTrue = true;
        Object oFalse = false;

        DataTable _groups;
        object oClassType = "Excel.Chart.8";

        string _exam;
        string _subject;
        string _report_type;

        ArrayList _totaldata;
        string _addr;
        Dictionary<string, List<string>> _groups_group;

            
        public Partition_wordcreator(ArrayList sdata, DataTable groups, Dictionary<string, List<string>> groups_group):this(null, sdata, groups, groups_group)
        {
        }
        public Partition_wordcreator(ArrayList totaldata, ArrayList sdata, DataTable groups, Dictionary<string, List<string>> groups_group)
        {
            _totaldata = totaldata;
            _sdata = sdata;
            _groups = groups;
            _exam = Utils.exam;
            _subject = Utils.subject;
            _report_type = Utils.report_style;

            _groups_group = groups_group;
        }
        public void creating_ZH_QX_word(ArrayList ZH_totaldata, ArrayList ZH_sdata, DataTable ZH_group, Dictionary<string, List<string>> wenli_group)
        {
            string subject = Utils.subject;
            object filepath = @Utils.CurrentDirectory + @"\template2.dotx";
            //Start Word and create a new document.
            _addr = Utils.save_address + @"\" + subject + ".docx";
            oWord = new Word.Application();

            oWord.Visible = Utils.isVisible;
            oDoc = oWord.Documents.Add(ref filepath, ref oMissing,
            ref oMissing, ref oMissing);
            Utils.WriteFrontPage(oDoc);

            insertText(ExamTitle0, "  整体统计分析");
            insertText(ExamTitle1, "总体分析");
            insertTotalTable("    总分分析表", ZH_totaldata);
            ChartCombine chartdata = new ChartCombine();
            foreach (PartitionData temp in ZH_sdata)
            {
                chartdata.Add(temp.total_dist, temp.name);
            }
            insertChart("    总分分布曲线图", chartdata.target, "分数", "人数", Excel.XlChartType.xlLineMarkers);
            insertGroupTopic("    题组整体分析表", ZH_totaldata, 3);
            insertFreqTable("    总分频率分布", ZH_sdata);
            insertText(ExamTitle1, "题组分析");
            List<string> keys = new List<string>(wenli_group.Keys);
            int group_count = 3;
            for (int i = 1; i < wenli_group.Count; i++)
            {
                insertText(ExamTitle2, keys[i]);
                foreach (string group in wenli_group[keys[i]])
                {
                    insertText(ExamTitle3, group);
                    insertTH(ZH_group.Rows[group_count]["th"].ToString().Trim());
                    insertSingleGroupTotal("    " + group + "总分分析表", group_count, true, ZH_sdata);
                    ChartCombine tempdata = new ChartCombine();
                    for (int j = 0; j < ZH_sdata.Count; j++)
                    {
                        tempdata.Add(getGroupData(ZH_sdata, j, group_count).group_dist, ((PartitionData)ZH_sdata[j]).name);
                    }
                    insertChart("    " + group + "分数分布图", tempdata.target, "分数", "比率(%)", Excel.XlChartType.xlLineMarkers);
                    insertSingleGrouptuple("    " + group + "分组分析表", group_count, ZH_sdata);
                    group_count++;
                }
            }
            //for (int i = 3; i < ((PartitionData)ZH_sdata[ZH_sdata.Count - 1]).groups_analysis.Rows.Count; i++)
            //{
            //    insertText(ExamTitle2, getGroupName(ZH_sdata, ZH_sdata.Count - 1, i));
            //    insertText(ExamBodyText, "本题组包含题号：" + ZH_group.Rows[i]["th"].ToString().Trim());
            //    insertSingleGroupTotal(getGroupName(ZH_sdata, ZH_sdata.Count - 1, i) + "总分分析表", i, true, ZH_sdata);
            //    ChartCombine tempdata = new ChartCombine();
            //    for (int j = 0; j < ZH_sdata.Count; j++)
            //    {
            //        tempdata.Add(getGroupData(ZH_sdata, j, i).group_dist, ((PartitionData)ZH_sdata[j]).name);
            //    }
            //    insertChart(getGroupName(ZH_sdata, ZH_sdata.Count - 1, i) + "分数分布图", tempdata.target, "分数", "频率(%)", Excel.XlChartType.xlLineMarkers);
            //    insertSingleGrouptuple(ZH_group.Rows[i]["tz"].ToString().Trim() + "分组分析表", i, ZH_sdata);
            //}
            insertText(ExamTitle0, " " + Utils.subject.Substring(3) + "统计分析");
            creating_word_part2();
        }
        public void creating_ZH_word(ArrayList ZH_sdata, DataTable ZH_group, Dictionary<string, List<string>> wenli_group)
        {
            object filepath = @Utils.CurrentDirectory + @"\template2.dotx";
            //Start Word and create a new document.

            oWord = new Word.Application();

            oWord.Visible = Utils.isVisible;
            oDoc = oWord.Documents.Add(ref filepath, ref oMissing,
            ref oMissing, ref oMissing);
            Utils.WriteFrontPage(oDoc);

            insertText(ExamTitle0, " 整体统计分析");
            insertText(ExamTitle1, "总体分析");
            insertTotalTable("    总分分析表", ZH_sdata);
            ChartCombine chartdata = new ChartCombine();
            foreach (PartitionData temp in ZH_sdata)
            {
                chartdata.Add(temp.total_dist, temp.name);
            }
            insertChart("    总分分布曲线图", chartdata.target, "分数", "人数", Excel.XlChartType.xlXYScatterLines);
            insertGroupTopic("    题组整体分析表", ZH_sdata, 3);
            insertFreqTable("    总分频率分布", ZH_sdata);
            insertText(ExamTitle1, "题组分析");
            List<string> keys = new List<string>(wenli_group.Keys);
            int group_count = 3;
            for (int i = 1; i < wenli_group.Count; i++)
            {
                insertText(ExamTitle2, keys[i]);
                foreach (string group in wenli_group[keys[i]])
                {
                    insertText(ExamTitle3, group);
                    insertTH(ZH_group.Rows[group_count]["th"].ToString().Trim());
                    insertSingleGroupTotal("    " + group + "总分分析表", group_count, true, ZH_sdata);
                    ChartCombine tempdata = new ChartCombine();
                    for (int j = 0; j < ZH_sdata.Count; j++)
                    {
                        tempdata.Add(getGroupData(ZH_sdata, j, group_count).group_dist, ((PartitionData)ZH_sdata[j]).name);
                    }
                    insertChart("    " + group + "分数分布图", tempdata.target, "分数", "比率(%)", Excel.XlChartType.xlLineMarkers);
                    insertSingleGrouptuple("    " + group + "分组分析表", group_count, ZH_sdata);
                    group_count++;
                }
            }
            //for (int i = 3; i < ((PartitionData)ZH_sdata[ZH_sdata.Count - 1]).groups_analysis.Rows.Count; i++)
            //{
            //    insertText(ExamTitle2, getGroupName(ZH_sdata, ZH_sdata.Count - 1, i));
            //    insertText(ExamBodyText, "本题组包含题号：" + ZH_group.Rows[i]["th"].ToString().Trim());
            //    insertSingleGroupTotal(getGroupName(ZH_sdata, ZH_sdata.Count - 1, i) + "总分分析表", i, true, ZH_sdata);
            //    ChartCombine tempdata = new ChartCombine();
            //    for (int j = 0; j < ZH_sdata.Count; j++)
            //    {
            //        tempdata.Add(getGroupData(ZH_sdata, j, i).group_dist, ((PartitionData)ZH_sdata[j]).name);
            //    }
            //    insertChart(getGroupName(ZH_sdata, ZH_sdata.Count - 1, i) + "分数分布图", tempdata.target, "分数", "频率(%)", Excel.XlChartType.xlLineMarkers);
            //    insertSingleGrouptuple(ZH_group.Rows[i]["tz"].ToString().Trim() + "分组分析表", i, ZH_sdata);
            //}
            insertText(ExamTitle0, " " + Utils.subject.Substring(3) + "统计分析");
            creating_word_part2();
        }
        public void creating_word()
        {
            object filepath = @Utils.CurrentDirectory + @"\template.dotx";
            //Start Word and create a new document.

            oWord = new Word.Application();

            oWord.Visible = Utils.isVisible;
            oDoc = oWord.Documents.Add(ref filepath, ref oMissing,
            ref oMissing, ref oMissing);
            Utils.WriteFrontPage(oDoc);

            object oPageBreak = Microsoft.Office.Interop.Word.WdBreakType.wdPageBreak;
            if (Utils.WSLG)
                creating_word_part3();
            else
                creating_word_part2();
            

            
        }
        public void creating_word_part3()
        {
            insertText(ExamTitle1, "总体分析");
            insertTotalTable("    总分分析表", _sdata);


            ChartCombine chartdata = new ChartCombine();
            foreach (PartitionData temp in _sdata)
            {
                chartdata.Add(temp.total_dist, temp.name);
            }
            insertChart("    总分分布曲线图", chartdata.target, "分数", "人数", Excel.XlChartType.xlLineMarkers);
            insertTotalTopic("    题目整体分析表", _sdata);
            insertGroupTopic("    题组整体分析表", _sdata, ((PartitionData)_sdata[_sdata.Count - 1]).groups_analysis.Rows.Count);

            insertFreqTable("    总分频率分布", _sdata);

            insertText(ExamTitle1, "题组分析");
            List<string> keys = new List<string>(_groups_group.Keys);
            int group_count = 0;
            for (int i = 0; i < _groups_group.Count; i++)
            {
                insertText(ExamTitle2, keys[i]);
                foreach (string group in _groups_group[keys[i]])
                {
                    insertText(ExamTitle3, group);
                    insertTH(_groups.Rows[group_count]["th"].ToString().Trim());
                    insertSingleGroupTotal("    " + group + "总分分析表", group_count, true, _sdata);
                    ChartCombine tempdata = new ChartCombine();
                    for (int j = 0; j < _sdata.Count; j++)
                    {
                        tempdata.Add(getGroupData(_sdata, j, group_count).group_dist, ((PartitionData)_sdata[j]).name);
                    }
                    insertChart("    " + group + "分数分布图", tempdata.target, "分数", "比率(%)", Excel.XlChartType.xlLineMarkers);
                    insertSingleGrouptuple("    " + group + "分组分析表", group_count, _sdata);
                    group_count++;
                }
            }
            //for (int i = 0; i < ((PartitionData)_sdata[_sdata.Count - 1]).groups_analysis.Rows.Count; i++)
            //{
            //    insertText(ExamTitle2, getGroupName(_sdata, _sdata.Count - 1, i));
            //    insertText(ExamBodyText, "本题组包含题号：" + _groups.Rows[i]["th"].ToString().Trim());
            //    insertSingleGroupTotal(getGroupName(_sdata, _sdata.Count - 1, i) + "总分分析表", i, true, _sdata);
            //    ChartCombine tempdata = new ChartCombine();
            //    for (int j = 0; j < _sdata.Count; j++)
            //    {
            //        tempdata.Add(getGroupData(_sdata, j, i).group_dist, ((PartitionData)_sdata[j]).name);
            //    }
            //    insertChart(getGroupName(_sdata, _sdata.Count - 1, i) + "分数分布图", tempdata.target, "分数", "频率(%)", Excel.XlChartType.xlLineMarkers);
            //    insertSingleGrouptuple(_groups.Rows[i]["tz"].ToString().Trim() + "分组分析表", i, _sdata);
            //}
            insertText(ExamTitle1, "题目分析");
            for (int i = 0; i < ((PartitionData)_sdata[_sdata.Count - 1]).total_analysis.Rows.Count; i++)
            {
                string numstr = "第" + getSingleName(_sdata.Count - 1, i).Substring(1) + "题";
                insertText(ExamTitle3, numstr);
                insertSingleGroupTotal("    " + numstr + "分析表", i, false, _sdata);
                insertSingleTopictuple("    " + numstr + "分组分析表", i, getSingleData(_sdata.Count - 1, i).stype);
            }

            foreach (Word.TableOfContents table in oDoc.TablesOfContents)
                table.Update();
            Utils.Save(oDoc, oWord);
        }
        public void creating_word_part2()
        {
            insertText(ExamTitle1, "总体分析");
            if (_report_type.Equals("区县"))
                insertTotalTable("    总分分析表", _totaldata);
            else if (_report_type.Equals("两类示范校") || _report_type.Equals("城郊"))
                insertTotalTable("    总分分析表", _sdata);


            ChartCombine chartdata = new ChartCombine();
            foreach (PartitionData temp in _sdata)
            {
                chartdata.Add(temp.total_dist, temp.name);
            }
            insertChart("    总分分布曲线图", chartdata.target, "分数", "人数", Excel.XlChartType.xlLineMarkers);
            if (_report_type.Equals("区县"))
            {
                insertTotalTopic("    题目整体分析表", _totaldata);
                insertGroupTopic("    题组整体分析表", _totaldata, ((PartitionData)_totaldata[_totaldata.Count - 1]).groups_analysis.Rows.Count);
            }
            else if (_report_type.Equals("两类示范校") || _report_type.Equals("城郊"))
            {
                insertTotalTopic("    题目整体分析表", _sdata);
                insertGroupTopic("    题组整体分析表", _sdata, ((PartitionData)_sdata[_sdata.Count - 1]).groups_analysis.Rows.Count);
            }
            insertFreqTable("    总分频率分布", _sdata);

            insertText(ExamTitle1, "题组分析");
            List<string> keys = new List<string>(_groups_group.Keys);
            int group_count = 0;
            for (int i = 0; i < _groups_group.Count; i++)
            {
                insertText(ExamTitle2, keys[i]);
                foreach (string group in _groups_group[keys[i]])
                {
                    insertText(ExamTitle3, group);
                    insertTH(_groups.Rows[group_count]["th"].ToString().Trim());
                    insertSingleGroupTotal("    " + group + "总分分析表", group_count, true, _sdata);
                    ChartCombine tempdata = new ChartCombine();
                    for (int j = 0; j < _sdata.Count; j++)
                    {
                        tempdata.Add(getGroupData(_sdata, j, group_count).group_dist, ((PartitionData)_sdata[j]).name);
                    }
                    insertChart("    " + group + "分数分布图", tempdata.target, "分数", "比率(%)", Excel.XlChartType.xlLineMarkers);
                    insertSingleGrouptuple("    " + group + "分组分析表", group_count, _sdata);
                    group_count++;
                }
            }
            //for (int i = 0; i < ((PartitionData)_sdata[_sdata.Count - 1]).groups_analysis.Rows.Count; i++)
            //{
            //    insertText(ExamTitle2, getGroupName(_sdata, _sdata.Count - 1, i));
            //    insertText(ExamBodyText, "本题组包含题号：" + _groups.Rows[i]["th"].ToString().Trim());
            //    insertSingleGroupTotal(getGroupName(_sdata, _sdata.Count - 1, i) + "总分分析表", i, true, _sdata);
            //    ChartCombine tempdata = new ChartCombine();
            //    for (int j = 0; j < _sdata.Count; j++)
            //    {
            //        tempdata.Add(getGroupData(_sdata, j, i).group_dist, ((PartitionData)_sdata[j]).name);
            //    }
            //    insertChart(getGroupName(_sdata, _sdata.Count - 1, i) + "分数分布图", tempdata.target, "分数", "频率(%)", Excel.XlChartType.xlLineMarkers);
            //    insertSingleGrouptuple(_groups.Rows[i]["tz"].ToString().Trim() + "分组分析表", i, _sdata);
            //}
            insertText(ExamTitle1, "题目分析");
            for (int i = 0; i < ((PartitionData)_sdata[_sdata.Count - 1]).total_analysis.Rows.Count; i++)
            {
                string numstr = "第" + getSingleName(_sdata.Count - 1, i).Substring(1) + "题";
                insertText(ExamTitle3, numstr);
                insertSingleGroupTotal("    " + numstr + "分析表", i, false, _sdata);
                insertSingleTopictuple("    " + numstr + "分组分析表", i, getSingleData(_sdata.Count - 1, i).stype);
            }

            foreach (Word.TableOfContents table in oDoc.TablesOfContents)
                table.Update();
            Utils.Save(oDoc, oWord);

        }

        public void insertSingleTopictuple(string title, int topicnum, WordData.single_type type)
        {
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            int rownum = getSingleData(_sdata.Count - 1, topicnum).single_detail.Rows.Count;
            int colnum = getSingleData(_sdata.Count - 1, topicnum).single_detail.Columns.Count;
            table = oDoc.Tables.Add(range, rownum * _sdata.Count + 1, colnum + 1, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            DataTable stand = getSingleData(_sdata.Count - 1, topicnum).single_detail;
            if (type == WordData.single_type.single || type == WordData.single_type.multiple)
                table.Cell(1, 1).Range.Text = "选项";
            else
                table.Cell(1, 1).Range.Text = "分值";
            table.Cell(1, 2).Range.Text = "分类";
            for (int i = 1; i < colnum; i++)
            {
                if (stand.Columns[i].ColumnName.Trim().Equals("frequency"))
                    table.Cell(1, i + 2).Range.Text = "人数";
                else if (stand.Columns[i].ColumnName.Trim().Equals("rate"))
                    table.Cell(1, i + 2).Range.Text = "比率(%)";
                else if (stand.Columns[i].ColumnName.Trim().Equals("avg"))
                {
                    table.Cell(1, i + 2).Range.Text = "平均值";
                }
                else
                    table.Cell(1, i + 2).Range.Text = stand.Columns[i].ColumnName;
            }
            int row = 2;
            for (int i = 0; i < stand.Rows.Count; i++)
            {
                string primarykey = stand.Rows[i]["mark"].ToString().Trim();
                for (int k = 0; k < _sdata.Count; k++)
                {
                    if (getSingleData(k, topicnum).single_detail.Rows.Contains(primarykey))
                    {
                        DataRow dr = getSingleData(k, topicnum).single_detail.Rows.Find(primarykey);
                        if (dr["mark"].ToString().Trim().Equals("未选") || dr["mark"].ToString().Trim().Equals("未选或多选"))
                        {
                            int j;
                            int temp_rownum = (rownum - 3) * _sdata.Count + 2 + k;
                            table.Cell(temp_rownum, 1).Range.Text = dr["mark"].ToString().Trim();
                            table.Cell(temp_rownum, 2).Range.Text = ((PartitionData)_sdata[k]).name;
                            for (j = 1; j < colnum - 3; j++)
                                table.Cell(temp_rownum, j + 2).Range.Text = Convert.ToInt32(dr[j]).ToString();
                            table.Cell(temp_rownum, j + 2).Range.Text = dr[j].ToString();
                            table.Cell(temp_rownum, j + 3).Range.Text = string.Format("{0:F2}", dr[j + 1]);
                            table.Cell(temp_rownum, j + 4).Range.Text = string.Format("{0:F2}", dr[j + 2]);


                        }
                        else if (dr["mark"].ToString().Trim().Equals("合计"))
                        {
                            int j;
                            int temp_rownum = (rownum - 2) * _sdata.Count + 2 + k;
                            table.Cell(temp_rownum, 1).Range.Text = dr["mark"].ToString().Trim();
                            table.Cell(temp_rownum, 2).Range.Text = ((PartitionData)_sdata[k]).name;
                            for (j = 1; j < colnum - 3; j++)
                                table.Cell(temp_rownum, j + 2).Range.Text = Convert.ToInt32(dr[j]).ToString();
                            table.Cell(temp_rownum, j + 2).Range.Text = dr[j].ToString();
                            table.Cell(temp_rownum, j + 3).Range.Text = string.Format("{0:F2}", dr[j + 1]);
                            table.Cell(temp_rownum, j + 4).Range.Text = string.Format("{0:F2}", ((PartitionData)_sdata[k]).avg);


                        }
                        else if (dr["mark"].ToString().Trim().Equals("得分率"))
                        {
                            int j;
                            int temp_rownum = (rownum - 1) * _sdata.Count + 2 + k;
                            table.Cell(temp_rownum, 1).Range.Text = dr["mark"].ToString().Trim();
                            table.Cell(temp_rownum, 2).Range.Text = ((PartitionData)_sdata[k]).name;
                            for (j = 1; j < colnum - 3; j++)
                                table.Cell(temp_rownum, j + 2).Range.Text = string.Format("{0:F2}", dr[j]);
                            table.Cell(temp_rownum, j + 2).Range.Text = string.Format("{0:F2}", ((PartitionData)_sdata[k]).total_analysis.Rows[topicnum]["difficulty"]);
                            table.Cell(temp_rownum, j + 3).Range.Text = "-";
                            table.Cell(temp_rownum, j + 4).Range.Text = "-";

                        }
                        else
                        {
                            int j;

                            table.Cell(row, 1).Range.Text = dr["mark"].ToString().Trim();
                            table.Cell(row, 2).Range.Text = ((PartitionData)_sdata[k]).name;
                            for (j = 1; j < colnum - 3; j++)
                                table.Cell(row, j + 2).Range.Text = Convert.ToInt32(dr[j]).ToString();
                            table.Cell(row, j + 2).Range.Text = dr[j].ToString();
                            table.Cell(row, j + 3).Range.Text = string.Format("{0:F2}", dr[j + 1]);
                            table.Cell(row, j + 4).Range.Text = string.Format("{0:F2}", dr[j + 2]);

                            row++;
                        }

                    }
                    else
                    {
                        DataRow dr = stand.Rows[i];
                        if (dr["mark"].ToString().Trim().Equals("未选") || dr["mark"].ToString().Trim().Equals("未选或多选"))
                        {
                            int j;
                            int temp_rownum = (rownum - 3) * _sdata.Count + 2 + k;
                            table.Cell(temp_rownum, 1).Range.Text = dr["mark"].ToString().Trim();
                            table.Cell(temp_rownum, 2).Range.Text = ((PartitionData)_sdata[k]).name;
                            for (j = 1; j < colnum - 3; j++)
                                table.Cell(temp_rownum, j + 2).Range.Text = "0";
                            table.Cell(temp_rownum, j + 2).Range.Text = "0";
                            table.Cell(temp_rownum, j + 3).Range.Text = "0.00";
                            table.Cell(temp_rownum, j + 4).Range.Text = "0.00";


                        }
                        else
                        {
                            int j;

                            table.Cell(row, 1).Range.Text = dr["mark"].ToString().Trim();
                            table.Cell(row, 2).Range.Text = ((PartitionData)_sdata[k]).name;
                            for (j = 1; j < colnum - 3; j++)
                                table.Cell(row, j + 2).Range.Text = "0";
                            table.Cell(row, j + 2).Range.Text = "0";
                            table.Cell(row, j + 3).Range.Text = "0.00";
                            table.Cell(row, j + 4).Range.Text = "0.00";

                            row++;
                        }
                    }
                }
            }


            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            verticalCellMerge(table, 2, 1);
            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();
        }
        public PartitionData.single_data getSingleData(int total, int row)
        {
            return (PartitionData.single_data)((PartitionData)_sdata[total]).single_topic_analysis[row];
        }
        public string getSingleName(int total, int row)
        {
            return ((PartitionData)_sdata[total]).total_analysis.Rows[row]["number"].ToString().Trim();
        }
        public void insertSingleGrouptuple(string title, int group, ArrayList sdata)
        {
            int count = getGroupData(sdata, sdata.Count - 1, group).group_detail.Rows.Count;
            int col = getGroupData(sdata, sdata.Count - 1, group).group_detail.Columns.Count;
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            table = oDoc.Tables.Add(range, count * sdata.Count + 1, col + 1, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            int colnum = 0;
            table.Cell(1, 1).Range.Text = "分值";
            table.Cell(1, 2).Range.Text = "分类";
            for (colnum = 1; colnum < col - 3; colnum++)
                table.Cell(1, colnum + 2).Range.Text = getGroupData(sdata, sdata.Count - 1, group).group_detail.Columns[colnum].ColumnName.Trim();
            table.Cell(1, colnum + 2).Range.Text = "人数";
            table.Cell(1, colnum + 3).Range.Text = "比率(%)";
            table.Cell(1, colnum + 4).Range.Text = "平均值";

            for (int i = 0; i < count; i++)
            {
                int tablerow = i * sdata.Count + 2;
                string primarykey = getGroupData(sdata, sdata.Count - 1, group).group_detail.Rows[i]["mark"].ToString().Trim();
                for(int j = 0; j < sdata.Count; j++)
                {
                    DataTable dt = getGroupData(sdata, j, group).group_detail;
                    if (dt.Rows.Contains(primarykey))
                    {
                        DataRow dr = dt.Rows.Find(primarykey);
                        if (dr["mark"].ToString().Trim().Equals("合计"))
                        {
                            int k = 1;
                            table.Cell(tablerow, 1).Range.Text = dr["mark"].ToString();
                            table.Cell(tablerow, 2).Range.Text = ((PartitionData)sdata[j]).name;
                            for (k = 1; k < col - 3; k++)
                                table.Cell(tablerow, k + 2).Range.Text = Convert.ToInt32(dr[k]).ToString();
                            table.Cell(tablerow, k + 2).Range.Text = Convert.ToInt32(dr[k]).ToString();
                            table.Cell(tablerow, k + 3).Range.Text = string.Format("{0:F2}", dr[k + 1]);
                            table.Cell(tablerow, k + 4).Range.Text = string.Format("{0:F2}", dr[k + 2]);
                        }
                        else if (dr["mark"].ToString().Trim().Equals("得分率"))
                        {
                            int k = 1;
                            table.Cell(tablerow, 1).Range.Text = dr["mark"].ToString();
                            table.Cell(tablerow, 2).Range.Text = ((PartitionData)sdata[j]).name;
                            for (k = 1; k < col - 3; k++)
                                table.Cell(tablerow, k + 2).Range.Text = string.Format("{0:F2}", dr[k]);
                            table.Cell(tablerow, k + 2).Range.Text = string.Format("{0:F2}", ((PartitionData)sdata[j]).groups_analysis.Rows[group]["difficulty"]);
                            table.Cell(tablerow, k + 3).Range.Text = "-";
                            table.Cell(tablerow, k + 4).Range.Text = "-";
                        }
                        else
                        {
                            int k = 1;
                            table.Cell(tablerow, 1).Range.Text = dt.Rows.Find(primarykey)["mark"].ToString();
                            table.Cell(tablerow, 2).Range.Text = ((PartitionData)sdata[j]).name;
                            for (k = 1; k < col - 3; k++)
                                table.Cell(tablerow, k + 2).Range.Text = Convert.ToInt32(dt.Rows.Find(primarykey)[k]).ToString();
                            table.Cell(tablerow, k + 2).Range.Text = dt.Rows.Find(primarykey)[k].ToString();
                            table.Cell(tablerow, k + 3).Range.Text = string.Format("{0:F2}", dt.Rows.Find(primarykey)[k + 1]);
                            table.Cell(tablerow, k + 4).Range.Text = string.Format("{0:F2}", dt.Rows.Find(primarykey)[k + 2]);
                        }
                        
                    }
                    else
                    {
                        int k = 1;
                        table.Cell(tablerow, 1).Range.Text = primarykey;
                        table.Cell(tablerow, 2).Range.Text = ((PartitionData)sdata[j]).name;
                        for (k = 1; k < col - 3; k++)
                            table.Cell(tablerow, k + 2).Range.Text = "0";
                        table.Cell(tablerow, k + 2).Range.Text = "0";
                        table.Cell(tablerow, k + 3).Range.Text = "0.00";
                        table.Cell(tablerow, k + 4).Range.Text = "0.00";
                    }
                    tablerow++;
                }
            }
            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            verticalCellMerge(table, 2, 1);


            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();

        }

        public void insertSingleGroupTotal(string title, int total, bool isGroup, ArrayList sdata)
        {
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            int col;
            if (Utils.WSLG)
                col = 10;
            else
                col = 9;
            table = oDoc.Tables.Add(range, sdata.Count + 1, col, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            table.Cell(1, 1).Range.Text = "分类";
            table.Cell(1, 2).Range.Text = "人数";
            table.Cell(1, 3).Range.Text = "满分值";
            table.Cell(1, 4).Range.Text = "最大值";
            table.Cell(1, 5).Range.Text = "最小值";
            table.Cell(1, 6).Range.Text = "平均值";
            table.Cell(1, 7).Range.Text = "标准差";
            table.Cell(1, 8).Range.Text = "差异系数";
            table.Cell(1, 9).Range.Text = "得分率";
            if(Utils.WSLG)
                table.Cell(1, 10).Range.Text = "鉴别指数";

            for (int i = 0; i < sdata.Count; i++)
            {
                DataTable data;
                PartitionData partition = (PartitionData)sdata[i];
                if (isGroup)
                    data = partition.groups_analysis;
                else
                    data = partition.total_analysis;
                table.Cell(i + 2, 1).Range.Text = partition.name;
                table.Cell(i + 2, 2).Range.Text = partition.total_num.ToString();
                table.Cell(i + 2, 3).Range.Text = Convert.ToInt32(data.Rows[total]["fullmark"]).ToString();
                table.Cell(i + 2, 4).Range.Text = string.Format("{0:F1}", data.Rows[total]["max"]);
                table.Cell(i + 2, 5).Range.Text = string.Format("{0:F1}", data.Rows[total]["min"]);
                table.Cell(i + 2, 6).Range.Text = string.Format("{0:F2}", data.Rows[total]["avg"]);
                table.Cell(i + 2, 7).Range.Text = string.Format("{0:F2}", data.Rows[total]["stDev"]);
                table.Cell(i + 2, 8).Range.Text = string.Format("{0:F2}", data.Rows[total]["dfactor"]);
                table.Cell(i + 2, 9).Range.Text = string.Format("{0:F2}", data.Rows[total]["difficulty"]);
                if (Utils.WSLG)
                {
                    if(isGroup)
                        table.Cell(i + 2, 10).Range.Text = string.Format("{0:F2}", ((WSLG_partitiondata)partition).group_discriminant[total]);
                    else
                        table.Cell(i + 2, 10).Range.Text = string.Format("{0:F2}", ((WSLG_partitiondata)partition).total_discriminant[total]);
                }
            }
            table.Select();
            oWord.Selection.set_Style(ref TableContent2);
            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();
        }

        public string getGroupName(ArrayList sdata, int total, int row)
        {
            return (string)((PartitionData)sdata[total]).groups_analysis.Rows[row]["number"];
        }
        public PartitionData.group_data getGroupData(ArrayList sdata, int total, int group)
        {
            return (PartitionData.group_data)((PartitionData)sdata[total]).single_group_analysis[group];
        }
        public void insertFreqTable(string title, ArrayList sdata)
        {
            int count = ((PartitionData)sdata[sdata.Count - 1]).freq_analysis.Rows.Count;
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            table = oDoc.Tables.Add(range, sdata.Count * count + 1, 6, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            table.Cell(1, 1).Range.Text = "分值";
            table.Cell(1, 2).Range.Text = "分类";
            table.Cell(1, 3).Range.Text = "人数";
            table.Cell(1, 4).Range.Text = "比率(%)";
            table.Cell(1, 5).Range.Text = "累计人数";
            table.Cell(1, 6).Range.Text = "累计比率(%)";

            string[] lastFreq = new string[sdata.Count];
            string[] lastRate = new string[sdata.Count];
            for (int i = 0; i < sdata.Count; i++)
            {
                lastFreq[i] = "0";
                lastRate[i] = "0.00";
            }
            for (int i = 0; i < count; i++)
            {
                int tablerow = i * sdata.Count + 2;
                decimal primarykey = (decimal)((PartitionData)sdata[sdata.Count - 1]).freq_analysis.Rows[i]["totalmark"];
                int j = 0;
                foreach (PartitionData data in sdata)
                {
                    if (data.freq_analysis.Rows.Contains(primarykey))
                    {
                        table.Cell(tablerow, 1).Range.Text = string.Format("{0:F1}", data.freq_analysis.Rows.Find(primarykey)["totalmark"]);
                        table.Cell(tablerow, 2).Range.Text = data.name;
                        table.Cell(tablerow, 3).Range.Text = data.freq_analysis.Rows.Find(primarykey)["frequency"].ToString().Trim();
                        table.Cell(tablerow, 4).Range.Text = string.Format("{0:F2}", data.freq_analysis.Rows.Find(primarykey)["rate"]);
                        table.Cell(tablerow, 5).Range.Text = data.freq_analysis.Rows.Find(primarykey)["accumulateFreq"].ToString().Trim();
                        table.Cell(tablerow, 6).Range.Text = string.Format("{0:F2}", data.freq_analysis.Rows.Find(primarykey)["accumulateRate"]);

                        lastFreq[j] = data.freq_analysis.Rows.Find(primarykey)["accumulateFreq"].ToString().Trim();
                        lastRate[j] = string.Format("{0:F2}", data.freq_analysis.Rows.Find(primarykey)["accumulateRate"]);
                    }
                    else
                    {
                        table.Cell(tablerow, 1).Range.Text = string.Format("{0:F1}", primarykey);
                        table.Cell(tablerow, 2).Range.Text = data.name;
                        table.Cell(tablerow, 3).Range.Text = "0";
                        table.Cell(tablerow, 4).Range.Text = "0.00";
                        table.Cell(tablerow, 5).Range.Text = lastFreq[j];
                        table.Cell(tablerow, 6).Range.Text = lastRate[j];
                    }
                    tablerow++;
                    j++;
                }
            }

            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            verticalCellMerge(table, 2, 1);


            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();
        }
        public void insertGroupTopic(string title, ArrayList totaldata, int count)
        {
            //int count = ((PartitionData)totaldata[totaldata.Count - 1]).groups_analysis.Rows.Count;
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            int col;
            if (Utils.WSLG)
                col = 11;
            else
                col = 10;
            table = oDoc.Tables.Add(range, totaldata.Count * count + 1, col, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            table.Cell(1, 1).Range.Text = "题组";
            table.Cell(1, 2).Range.Text = "分类";
            table.Cell(1, 3).Range.Text = "人数";
            table.Cell(1, 4).Range.Text = "满分值";
            table.Cell(1, 5).Range.Text = "最大值";
            table.Cell(1, 6).Range.Text = "最小值";
            table.Cell(1, 7).Range.Text = "平均值";
            table.Cell(1, 8).Range.Text = "标准差";
            table.Cell(1, 9).Range.Text = "差异系数";
            table.Cell(1, 10).Range.Text = "得分率";
            if(Utils.WSLG)
                table.Cell(1, 11).Range.Text = "鉴别指数";

            for (int i = 0; i < count; i++)
            {
                int tablerow = i * totaldata.Count + 2;
                foreach (PartitionData data in totaldata)
                {
                    table.Cell(tablerow, 1).Range.Text = data.groups_analysis.Rows[i]["number"].ToString().Trim();
                    table.Cell(tablerow, 2).Range.Text = data.name;
                    table.Cell(tablerow, 3).Range.Text = data.total_num.ToString();
                    table.Cell(tablerow, 4).Range.Text = Convert.ToInt32(data.groups_analysis.Rows[i]["fullmark"]).ToString();
                    table.Cell(tablerow, 5).Range.Text = string.Format("{0:F1}", data.groups_analysis.Rows[i]["max"]);
                    table.Cell(tablerow, 6).Range.Text = string.Format("{0:F1}", data.groups_analysis.Rows[i]["min"]);
                    table.Cell(tablerow, 7).Range.Text = string.Format("{0:F2}", data.groups_analysis.Rows[i]["avg"]);
                    table.Cell(tablerow, 8).Range.Text = string.Format("{0:F2}", data.groups_analysis.Rows[i]["stDev"]);
                    table.Cell(tablerow, 9).Range.Text = string.Format("{0:F2}", data.groups_analysis.Rows[i]["dfactor"]);
                    table.Cell(tablerow, 10).Range.Text = string.Format("{0:F2}", data.groups_analysis.Rows[i]["difficulty"]);
                    
                    if(Utils.WSLG)
                        table.Cell(tablerow, 11).Range.Text = string.Format("{0:F2}", ((WSLG_partitiondata)data).group_discriminant[i]);
                    tablerow++;
                }
            }
            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            verticalCellMerge(table, 2, 1);


            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();
        }
        public void insertTotalTopic(string title, ArrayList totaldata)
        {
            int count = ((PartitionData)totaldata[totaldata.Count - 1]).total_analysis.Rows.Count;
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            int col;
            if (Utils.WSLG)
                col = 11;
            else
                col = 10;
            table = oDoc.Tables.Add(range, totaldata.Count * count + 1, col, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            
            table.Cell(1, 1).Range.Text = "题目";
            table.Cell(1, 2).Range.Text = "分类";
            table.Cell(1, 3).Range.Text = "人数";
            table.Cell(1, 4).Range.Text = "满分值";
            table.Cell(1, 5).Range.Text = "最大值";
            table.Cell(1, 6).Range.Text = "最小值";
            table.Cell(1, 7).Range.Text = "平均值";
            table.Cell(1, 8).Range.Text = "标准差";
            table.Cell(1, 9).Range.Text = "差异系数";
            table.Cell(1, 10).Range.Text = "得分率";
            if(Utils.WSLG)
                table.Cell(1, 11).Range.Text = "鉴别指数";

            for (int i = 0; i < count; i++)
            {
                int tablerow = i * totaldata.Count + 2;
                foreach (PartitionData data in totaldata)
                {
                    table.Cell(tablerow, 1).Range.Text = data.total_analysis.Rows[i]["number"].ToString().Trim().Substring(1);
                    table.Cell(tablerow, 2).Range.Text = data.name;
                    table.Cell(tablerow, 3).Range.Text = data.total_num.ToString();
                    table.Cell(tablerow, 4).Range.Text = Convert.ToInt32(data.total_analysis.Rows[i]["fullmark"]).ToString();
                    table.Cell(tablerow, 5).Range.Text = string.Format("{0:F1}", data.total_analysis.Rows[i]["max"]);
                    table.Cell(tablerow, 6).Range.Text = string.Format("{0:F1}", data.total_analysis.Rows[i]["min"]);
                    table.Cell(tablerow, 7).Range.Text = string.Format("{0:F2}", data.total_analysis.Rows[i]["avg"]);
                    table.Cell(tablerow, 8).Range.Text = string.Format("{0:F2}", data.total_analysis.Rows[i]["stDev"]);
                    table.Cell(tablerow, 9).Range.Text = string.Format("{0:F2}", data.total_analysis.Rows[i]["dfactor"]);
                    table.Cell(tablerow, 10).Range.Text = string.Format("{0:F2}", data.total_analysis.Rows[i]["difficulty"]);
                    if(Utils.WSLG)
                        table.Cell(tablerow, 11).Range.Text = string.Format("{0:F2}", ((WSLG_partitiondata)data).total_discriminant[i]);
                    tablerow++;
                }
            }
            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            verticalCellMerge(table, 2, 1);
            

            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();
        }
        private void verticalCellMerge(Word.Table table, int startRowIndex, int columnIndex)
        {
            string previousText = table.Cell(startRowIndex++, columnIndex).Range.Text;    // 保存对比文字
            int previousRowIndex = startRowIndex - 1;    // 因刚已经+1了，所以再减回去
            for (int i = startRowIndex; i <= table.Rows.Count; ++i) // 遍历所有行的columnIndex列，发现相同的合并，从起始行的下一行开始对比
            {
                string currentText = table.Cell(i, columnIndex).Range.Text;
                if (previousText.Equals(currentText))
                {
                    table.Cell(previousRowIndex, columnIndex).Merge(table.Cell(i, columnIndex)); // 合并先前单元格和当前单元格
                    //table.Cell(previousRowIndex, columnIndex).Select();
                    //oWord.Selection.Text = currentText.TrimEnd('\r');
                    string text = currentText.Trim('\a').Trim('\r');
                    table.Cell(previousRowIndex, columnIndex).Range.Text = text;   // 因为合并后并没有将单元格内容去除，需要手动修改

                    table.Cell(previousRowIndex, columnIndex).Select();
                    oWord.Selection.ParagraphFormat.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;    // 水平居中显示
                    table.Cell(previousRowIndex, columnIndex).VerticalAlignment = Word.WdCellVerticalAlignment.wdCellAlignVerticalCenter; // 垂直居中
                }
                else
                {
                    previousText = currentText; // 将对比文字替换为当前的内容
                    previousRowIndex = i;   // 检索到不同的内容，将当前行下标置为先前行下标，用于合并
                }
            }
        }
        public class ChartCombine
        {
            public DataTable target;
            int num = 2;
            public void Add(DataTable dt, string name)
            {
                if (target == null)
                {
                    target = dt.Copy();
                    target.Columns[0].ColumnName = "mark";
                    target.Columns[1].ColumnName = name;
                    return;
                }
                target.Columns.Add(name, typeof(decimal));
                for(int i = 0; i< target.Rows.Count; i++)
                {
                    target.Rows[i][name] = dt.Rows[i][1];
                }
                num++;
            } 
        }
        public void insertChart(string title, DataTable dt, string x_axis, string y_axis, object type)
        {
            if (dt.Columns.Count > 2)
            {
                ZedGraph.createMultipleCuve(dt, "分数", y_axis, Convert.ToDouble(dt.Compute("Min([" + dt.Columns[0].ColumnName + "])", "")), Convert.ToDouble(dt.Compute("Max([" + dt.Columns[0].ColumnName + "])", "")));
            }
            else
            {
                double[][] data = new double[dt.Rows.Count][];
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    data[i] = new double[2];
                    data[i][0] = Convert.ToDouble((decimal)dt.Rows[i][0]);
                    data[i][1] = Convert.ToDouble((decimal)dt.Rows[i][1]);

                }

                ZedGraph.createDiffCuve(data, Convert.ToDouble(dt.Compute("Min([" + dt.Columns[0].ColumnName + "])", "")), Convert.ToDouble(dt.Compute("Max([" + dt.Columns[0].ColumnName + "])", "")));
            }
            
            Word.Range dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
            dist_rng.Paste();
            dist_rng.InsertCaption(oWord.CaptionLabels["图"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            dist_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
            dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
            dist_rng.MoveStart(Word.WdUnits.wdParagraph, 1);
            dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            dist_rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            dist_rng.InsertParagraphAfter();
            //Excel.Application eapp = new Excel.Application();
            //eapp.Visible = false;
            //Excel.Workbooks wk = eapp.Workbooks;
            //Excel._Workbook _wk = wk.Add(oMissing);
            //Excel.Sheets shs = _wk.Sheets;

            //Word.InlineShape dist_shape;
            //Word.Range dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
            
            ////Excel.Workbook dist_book = (Excel.Workbook)dist_shape.OLEFormat.Object;
            //Excel.Worksheet dist_Sheet = shs.get_Item(1);

            //dist_Sheet.Cells.Clear();


            //object[,] data = new object[dt.Rows.Count + 2, dt.Columns.Count];
            //for (int i = 0; i < dt.Columns.Count; i++)
            //    data[0, i] = dt.Columns[i].ColumnName;
            //int in_row = 1;
            //foreach (DataRow dr1 in dt.Rows)
            //{
            //    int col = 0;
            //    foreach (var item in dr1.ItemArray)
            //    {
            //        data[in_row, col] = item;
            //        col++;
            //    }
            //    in_row++;
            //}
            //Excel.Range rng = dist_Sheet.Range[dist_Sheet.Cells[1][1], dist_Sheet.Cells[dt.Columns.Count][dt.Rows.Count + 1]];
            //rng.Select();
            //rng.Value2 = data;
            //Excel.Chart chart_dist = _wk.Charts.Add(oMissing, dist_Sheet, oMissing, oMissing);

            //Excel.Range dist_chart_rng = (Excel.Range)dist_Sheet.Cells[1, 1];

            //chart_dist.ChartWizard(rng, type, Type.Missing, Excel.XlRowCol.xlColumns, 1, 1, true, "", x_axis, y_axis, "");
            //FormatExcel(_wk, x_axis, y_axis);
            //dist_shape = dist_rng.InlineShapes.AddOLEObject(ref oClassType, _wk.Name,
            //ref oMissing, ref oMissing, ref oMissing,
            //ref oMissing, ref oMissing, ref oMissing);
            //dist_shape.Range.InsertCaption(oWord.CaptionLabels["图"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            //dist_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
            //dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            
            ////dist_Sheet.UsedRange.CopyPicture();
            //dist_shape.Width = 375;
            //dist_shape.Height = 220;
            ////dist_rng.PasteExcelTable(true, true, false);
            ////dist_shape.ConvertToShape();
            //dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
            //dist_rng.MoveStart(Word.WdUnits.wdParagraph, 1);
            //dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //dist_rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            //dist_rng.InsertParagraphAfter();
            //ReleaseExcel(_wk, eapp);
        }

        public void insertTotalTable(string title, ArrayList totaldata)
        {
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            int count;
            if (Utils.WSLG)
                count = 10;
            else
                count = 9;
            table = oDoc.Tables.Add(range, totaldata.Count + 1, count, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            table.Cell(1, 1).Range.Text = "分类";
            table.Cell(1, 2).Range.Text = "人数";
            table.Cell(1, 3).Range.Text = "满分值";
            table.Cell(1, 4).Range.Text = "最大值";
            table.Cell(1, 5).Range.Text = "最小值";
            table.Cell(1, 6).Range.Text = "平均值";
            table.Cell(1, 7).Range.Text = "标准差";
            table.Cell(1, 8).Range.Text = "差异系数";
            table.Cell(1, 9).Range.Text = "得分率";
            if(Utils.WSLG)
                table.Cell(1, 9).Range.Text = "鉴别指数";

            for (int i = 0; i < totaldata.Count; i++)
            {
                table.Cell(i + 2, 1).Range.Text = ((PartitionData)totaldata[i]).name;
                table.Cell(i + 2, 2).Range.Text = ((PartitionData)totaldata[i]).total_num.ToString();
                table.Cell(i + 2, 3).Range.Text = Convert.ToInt32(((PartitionData)totaldata[i]).fullmark).ToString();
                table.Cell(i + 2, 4).Range.Text = string.Format("{0:F1}", ((PartitionData)totaldata[i]).max);
                table.Cell(i + 2, 5).Range.Text = string.Format("{0:F1}", ((PartitionData)totaldata[i]).min);
                table.Cell(i + 2, 6).Range.Text = string.Format("{0:F2}", ((PartitionData)totaldata[i]).avg);
                table.Cell(i + 2, 7).Range.Text = string.Format("{0:F2}", ((PartitionData)totaldata[i]).stDev);
                table.Cell(i + 2, 8).Range.Text = string.Format("{0:F2}", ((PartitionData)totaldata[i]).Dfactor);
                table.Cell(i + 2, 9).Range.Text = string.Format("{0:F2}", ((PartitionData)totaldata[i]).difficulty);
                if(Utils.WSLG)
                    table.Cell(i + 2, 10).Range.Text = string.Format("{0:F2}", ((WSLG_partitiondata)totaldata[i]).discriminant);
            }

            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();

        }

        public void WriteIntoDocument(string BookmarkName, string FillName)
        {
            object bookmarkName = BookmarkName;
            Word.Bookmark bm = oDoc.Bookmarks.get_Item(ref bookmarkName);//返回书签 
            bm.Range.Text = FillName;//设置书签域的内容
        }

        public void insertText(object type, string content)
        {
            Word.Range first = oDoc.Paragraphs.Add(ref oMissing).Range;
            first.set_Style(type);
            first.InsertBefore(content + "\n");

            oDoc.Characters.Last.Select();
            oWord.Selection.HomeKey(Word.WdUnits.wdLine, oMissing);
            oWord.Selection.Delete(Word.WdUnits.wdCharacter, oMissing);
            oWord.Selection.Range.set_Style(ExamBodyText);
        }
        public void FormatExcel(Excel._Workbook _wk, string xstring, string ystring)
        {
            
            Excel.Axis yaxis = _wk.ActiveChart.Axes(Excel.XlAxisType.xlValue, Excel.XlAxisGroup.xlPrimary);
            _wk.ActiveChart.Legend.Format.TextFrame2.TextRange.Font.Size = 20;
            _wk.ActiveChart.Legend.Format.TextFrame2.TextRange.Font.Name = "Times New Roman";
            
            yaxis.HasTitle = true;
            yaxis.AxisTitle.Characters.Text = ystring;
            yaxis.AxisTitle.Characters.Font.Size = 20;
            yaxis.TickLabels.Font.Name = "Times New Roman";
            yaxis.TickLabels.Font.Size = 20;
            yaxis.MaximumScaleIsAuto = true;
            yaxis.MinimumScale = 0;
            Excel.Axis xaxis = _wk.ActiveChart.Axes(Excel.XlAxisType.xlCategory, Excel.XlAxisGroup.xlPrimary);

            xaxis.HasTitle = true;
            xaxis.AxisTitle.Characters.Text = xstring;
            xaxis.AxisTitle.Characters.Font.Size = 20;

            xaxis.TickLabels.Font.Name = "Times New Roman";
            xaxis.TickLabels.Font.Size = 20;
        }
        public void ReleaseExcel(Excel._Workbook wb, Excel.Application eapp)
        {
            wb.Close(false, oMissing, oMissing);
            wb = null;
            eapp.Quit();

            //Marshal.ReleaseComObject(wb);
            //Marshal.ReleaseComObject(eapp);

            KillSpecialExcel(eapp);
            eapp = null;
        }
        [DllImport("user32.dll", SetLastError = true)]

        static extern int GetWindowThreadProcessId(IntPtr hWnd, out int lpdwProcessId);
        public void KillSpecialExcel(Excel.Application app)
        {

            try
            {

                if (app != null)
                {

                    int lpdwProcessId = 0; ;

                    GetWindowThreadProcessId(new IntPtr(app.Hwnd), out lpdwProcessId);



                    System.Diagnostics.Process.GetProcessById(lpdwProcessId).Kill();

                }

            }

            catch (Exception ex)
            {

                Console.WriteLine("Delete Excel Process Error:" + ex.Message);

            }

        }

        void insertTH(string TH)
        {
            Word.Range first = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
            first.set_Style(ExamBodyText);
            first.InsertAfter("本题组共包含");
            int count = 0;
            string[] th_string = TH.ToString().Trim().Split(new char[2] { ',', '，' });
            foreach (string temp in th_string)
            {
                if (System.Text.RegularExpressions.Regex.IsMatch(temp, "^\\d+~\\d+$"))
                //if(th.Contains('~'))
                {
                    string[] num = temp.Split('~');
                    int j;
                    int size = Convert.ToInt32(num[0]) < Convert.ToInt32(num[1]) ? Convert.ToInt32(num[1]) : Convert.ToInt32(num[0]);
                    int start = Convert.ToInt32(num[0]) > Convert.ToInt32(num[1]) ? Convert.ToInt32(num[1]) : Convert.ToInt32(num[0]);
                    //此处需判断size和start的边界问题
                    for (j = start; j < size + 1; j++)
                    {
                        count++;
                    }

                }
                else
                    count++;
            }
            first.InsertAfter(count.ToString());
            first.InsertAfter("道试题，题号是：");
            for (int i = 0; i < th_string.Length; i++)
            {
                if (System.Text.RegularExpressions.Regex.IsMatch(th_string[i], "^\\d+~\\d+$"))
                {
                    string[] num = th_string[i].Split('~');
                    first.InsertAfter(num[0]);
                    first.InsertAfter("～");
                    first.InsertAfter(num[1]);
                }
                else
                    first.InsertAfter(th_string[i]);
                if (i < th_string.Length - 1)
                    first.InsertAfter("、");
            }
            first.InsertParagraphAfter();

            //oDoc.Characters.Last.Select();
            //oWord.Selection.HomeKey(Word.WdUnits.wdLine, oMissing);
            //oWord.Selection.Delete(Word.WdUnits.wdCharacter, oMissing);
            //oWord.Selection.Range.set_Style(ExamBodyText);
        }

        
    }
}
