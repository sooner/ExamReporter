using System;
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
using System.Windows.Forms;
using System.Threading;
using ZedGraph;
using Microsoft.Practices.EnterpriseLibrary.Common;


namespace ExamReport
{
    
    public class WordCreator
    {
        private object ExamTitle0 = "ExamTitle0";
        private object CaptionTitle = "CaptionTitle";
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
        WordData _sdata;
        WordData _ZH_data;
        object oParagrahbreak = Microsoft.Office.Interop.Word.WdBreakType.wdLineBreak;
        object oPagebreak = Microsoft.Office.Interop.Word.WdBreakType.wdPageBreak;
        Object oTrue = true;
        Object oFalse = false;
        bool isZonghe = false;

        object oClassType = "Excel.Chart.8";
        string _addr;
            
        public WordCreator(WordData sdata)
        {
            _sdata = sdata;
            _sdata._groups_ans.PrimaryKey = new DataColumn[] { _sdata._groups_ans.Columns["tz"] };
        }
        public WordCreator(WordData sdata, WordData ZH_data)
        {
            _sdata = sdata;
            _ZH_data = ZH_data;
            isZonghe = true;

            _sdata._groups_ans.PrimaryKey = new DataColumn[] { _sdata._groups_ans.Columns["tz"] };
        }
        public void creating_HK_word()
        {
            string subject = Utils.subject;
            HK_worddata data = (HK_worddata)_sdata;
            object filepath = @Utils.CurrentDirectory + @"\template.dotx";
            _addr = Utils.save_address + @"\" + Utils.subject + ".docx";
            oWord = new Word.Application();
            oWord.Visible = Utils.isVisible;
            oDoc = oWord.Documents.Add(ref filepath, ref oMissing,
            ref oMissing, ref oMissing);
            Utils.WriteFrontPage(oDoc);

            object oPageBreak = Microsoft.Office.Interop.Word.WdBreakType.wdPageBreak;
            insertText(ExamTitle1, "总体分析");
            insertHKTotalTable("    " + subject + "试卷总分分析表");
            insertHKTotalRank("    " + subject + "等级成绩分析表");
            insertTotalChart("    " + subject + "试卷总分分布曲线图", _sdata);
            insertHKTotalAnalysisTable("    " + subject + "题目、题组整体分析表");
            insertHKTotalRankTable("    " + subject + "题目、题组等级得分率分析表");
            insertHKFreq("    " + subject + "试卷总分次数分布表");

            insertText(ExamTitle1, "题组分析");
            int group_num = 0;
            foreach (string key in _sdata.groups_group.Keys)
            {
                insertText(ExamTitle2, key);
                List<string> groups = _sdata.groups_group[key];
                foreach (string group in groups)
                {
                    if (group.Equals("totalmark"))
                        continue;
                    insertText(ExamTitle3, group);
                    insertTH(_sdata._groups_ans.Rows.Find(group)["th"].ToString().Trim());
                    DataRow group_dr = _sdata.group_analysis.Rows.Find(group);

                    insertHKSingleGrouptable("    " + group + "总分分析表", _sdata.group_analysis.Rows[group_num]);
                    insertHKSingleGroupRank("    " + group + "等级得分率分析表", data.total_topic_rank.Rows[data.total_analysis.Rows.Count + group_num], (decimal)_sdata.group_analysis.Rows[group_num]["difficulty"]);
                    insertHKSingleGroupDistChart("    " + group + "分数分布曲线图", group_num);
                    insertHKSingleGroupDiffChart("    " + group + "难度曲线图", group_num);
                    insertHKSingleGroupAnalysisTable("    " + group + "分组分析表", group_num);
                    insertHKSingleGroupRankTable("    " + group + "等级分析表", data.single_group_rank[group_num]);
                    oDoc.Characters.Last.InsertBreak(oPagebreak);
                    group_num++;
                }
            }
            //for (int i = 0; i < _sdata.group_analysis.Rows.Count; i++)
            //{
            //    string groupID = _sdata.group_analysis.Rows[i]["number"].ToString();
            //    insertText(ExamTitle3, groupID);
            //    insertText(ExamBodyText, "本题组包含试题：" + _sdata._groups_ans.Rows[i]["th"]);
            //    insertHKSingleGrouptable(groupID + "总分分析表", _sdata.group_analysis.Rows[i]);
            //    insertHKSingleGroupRank(groupID + "等级得分率分析表", data.total_topic_rank.Rows[data.total_analysis.Rows.Count + i], (decimal)_sdata.group_analysis.Rows[i]["difficulty"]);
            //    insertHKSingleGroupDistChart(groupID + "分数分布曲线图", i);
            //    insertHKSingleGroupDiffChart(groupID + "难度曲线图", i);
            //    insertHKSingleGroupAnalysisTable(groupID + "分组分析表", i);
            //    insertHKSingleGroupRankTable(groupID + "等级分析表", data.single_group_rank[i]);
            //}

            insertText(ExamTitle1, "题目分析");
            for (int i = 0; i < _sdata.total_analysis.Rows.Count; i++)
            {
                string topicID = "第" + _sdata.total_analysis.Rows[i]["number"].ToString().Substring(1) + "题";
                insertText(ExamTitle3, topicID);
                insertHKSingleGrouptable("    " + topicID + "分析表", _sdata.total_analysis.Rows[i]);
                insertHKSingleGroupRank("    " + topicID + "等级得分率分析表", data.total_topic_rank.Rows[i], (decimal)_sdata.total_analysis.Rows[i]["difficulty"]);
                insertChart("    " + topicID + "难度曲线图", ((WordData.single_data)_sdata.single_topic_analysis[i]).single_difficulty, "分数", "难度", Excel.XlChartType.xlXYScatterSmooth);
                insertMultipleChart("    " + topicID + "分组难度曲线图", ((WordData.single_data)_sdata.single_topic_analysis[i]).single_dist, "分组", "难度", Excel.XlChartType.xlLineMarkers);
                insertGroupTable("    " + topicID + "分组分析表", ((WordData.single_data)_sdata.single_topic_analysis[i]).single_detail, ((WordData.single_data)_sdata.single_topic_analysis[i]).stype);
                insertHKSingleGroupRankTable("    " + topicID + "等级分析表", data.single_topic_rank[i]);
                oDoc.Characters.Last.InsertBreak(oPagebreak);

            }
            insertText(ExamTitle1, "相关分析");
            group_num = 0;
            foreach (string key in _sdata.groups_group.Keys)
            {
                insertText(ExamTitle3, key);
                insertCorTable(key, _sdata.group_cor[group_num]);
                group_num++;
            }
            foreach (Word.TableOfContents table in oDoc.TablesOfContents)
                table.Update();
            Utils.Save(oDoc, oWord);
            
        }
      

        public void insertHKSingleGroupRankTable(string title, DataTable dt)
        {
            HK_worddata data = (HK_worddata)_sdata;
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            table = oDoc.Tables.Add(range, dt.Rows.Count + 2, 11, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;

            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            table.Cell(1, 1).Range.Text = "分值";
            for (int i = 2; i < 7; i++)
                table.Cell(1, i).Merge(table.Cell(1, i + 1));
            table.Cell(1, 2).Range.Text = "优秀";
            table.Cell(1, 3).Range.Text = "良好";
            table.Cell(1, 4).Range.Text = "合格";
            table.Cell(1, 5).Range.Text = "不合格";
            table.Cell(1, 6).Range.Text = "全体";

            table.Cell(2, 2).Range.Text = "人数";
            table.Cell(2, 3).Range.Text = "比率";
            table.Cell(2, 4).Range.Text = "人数";
            table.Cell(2, 5).Range.Text = "比率";
            table.Cell(2, 6).Range.Text = "人数";
            table.Cell(2, 7).Range.Text = "比率";
            table.Cell(2, 8).Range.Text = "人数";
            table.Cell(2, 9).Range.Text = "比率";
            table.Cell(2, 10).Range.Text = "人数";
            table.Cell(2, 11).Range.Text = "比率";

            for(int i = 0; i < dt.Rows.Count; i++)
            {
                table.Cell(i+3, 1).Range.Text = dt.Rows[i]["mark"].ToString();
                for(int j = 1; j < dt.Columns.Count; j=j+2)
                {
                    table.Cell(i+3, j+1).Range.Text = dt.Rows[i][j].ToString();
                    table.Cell(i+3, j+2).Range.Text = string.Format("{0:F1}", dt.Rows[i][j+1]);
                }
            }

            table.Cell(1, 1).Merge(table.Cell(2, 1));
            table.Cell(1, 1).Select();
            oWord.Selection.ParagraphFormat.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;    // 水平居中显示
            table.Cell(1, 1).VerticalAlignment = Word.WdCellVerticalAlignment.wdCellAlignVerticalCenter;
            table.Select();
            oWord.Selection.set_Style(ref TableContent2);
            
            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();

        }
        public void insertHKSingleGroupAnalysisTable(string title, int group_num)
        {
            Word.Table single_group_analysis;
            Word.Range single_group_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;

            DataTable single_group_dt = ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_detail;
            single_group_analysis = oDoc.Tables.Add(single_group_rng, single_group_dt.Rows.Count + 1, single_group_dt.Columns.Count, ref oMissing, oTrue);

            single_group_analysis.Rows[1].HeadingFormat = -1;
            single_group_analysis.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            single_group_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
            single_group_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //single_group_analysis.Range.set_Style(TableContent2);
            //single_group_analysis.Range.Select();
            //oWord.Selection.Cells.VerticalAlignment = Word.WdCellVerticalAlignment.wdCellAlignVerticalCenter;
            //oWord.Selection.ParagraphFormat.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //single_group_analysis.Range.ParagraphFormat.Space1();
            single_group_analysis.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            single_group_analysis.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            //single_group_analysis.Range.Paragraphs.OutlineLevel = Word.WdOutlineLevel.wdOutlineLevelBodyText;
            //single_group_analysis.Range.Font.Size = 10;
            //single_group_analysis.Range.Font.Name = "黑体";
            int j = 0;
            for (int i = 0; i < single_group_dt.Columns.Count; i++)
            {
                if (single_group_dt.Columns[i].ColumnName.Trim().Equals("mark"))
                    single_group_analysis.Cell(1, i + 1).Range.Text = "分值";
                else if (single_group_dt.Columns[i].ColumnName.Trim().Equals("rate"))
                    single_group_analysis.Cell(1, i + 1).Range.Text = "比率(%)";
                else if (single_group_dt.Columns[i].ColumnName.Trim().Equals("avg"))
                    single_group_analysis.Cell(1, i + 1).Range.Text = "平均分";
                else if (single_group_dt.Columns[i].ColumnName.Trim().Equals("frequency"))
                    single_group_analysis.Cell(1, i + 1).Range.Text = "人数";
                else
                    single_group_analysis.Cell(1, i + 1).Range.Text = single_group_dt.Columns[i].ColumnName;
            }
            for (int i = 0; i < single_group_dt.Rows.Count; i++)
            {

                for (j = 0; j < single_group_dt.Columns.Count; j++)
                {
                    if (i == single_group_dt.Rows.Count - 1)
                    {
                        if (single_group_dt.Columns[j].ColumnName.Trim().Equals("rate") ||
                        single_group_dt.Columns[j].ColumnName.Trim().Equals("avg") || single_group_dt.Columns[j].ColumnName.Trim().Equals("frequency"))
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = "-";
                        else if (single_group_dt.Columns[j].ColumnName.Trim().Equals("mark"))
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = single_group_dt.Rows[i][j].ToString().Trim();

                        else
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = string.Format("{0:F2}", single_group_dt.Rows[i][j]);
                    }
                    else
                    {
                        if (single_group_dt.Columns[j].ColumnName.Trim().Equals("rate") ||
                            single_group_dt.Columns[j].ColumnName.Trim().Equals("avg"))
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = string.Format("{0:F2}", single_group_dt.Rows[i][j]);
                        else if (single_group_dt.Columns[j].ColumnName.Trim().Equals("mark"))
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = single_group_dt.Rows[i][j].ToString().Trim();
                        else if (single_group_dt.Columns[j].ColumnName.Trim().Equals("frequency"))
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = single_group_dt.Rows[i][j].ToString();
                        else
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = Convert.ToInt32(single_group_dt.Rows[i][j]).ToString().Trim();
                    }



                }
            }
            single_group_analysis.Select();
            oWord.Selection.set_Style(ref TableContent2);
            Word.Range single_table_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            single_table_range.InsertParagraphAfter();
        }
        public void insertHKSingleGroupDiffChart(string title, int group_num)
        {
            DataTable dt = ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_difficulty;
            double[][] data = new double[dt.Rows.Count][];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                data[i] = new double[2];
                data[i][0] = Convert.ToDouble((decimal)dt.Rows[i][0]);
                data[i][1] = Convert.ToDouble((decimal)dt.Rows[i][1]);

            }

            ZedGraph.createDiffCuve(data, Convert.ToDouble(dt.Compute("Min([" + dt.Columns[0].ColumnName + "])", "")), Convert.ToDouble(dt.Compute("Max([" + dt.Columns[0].ColumnName + "])", "")));
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

        //    Excel.Application eapp = new Excel.Application();
        //    eapp.Visible = false;
        //    Excel.Workbooks wk = eapp.Workbooks;
        //    Excel._Workbook _wk = wk.Add(oMissing);
        //    Excel.Sheets shs = _wk.Sheets;

        //    Word.InlineShape difficulty_shape;
        //    Word.Range difficulty_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;

            
            
        //    //Excel.Workbook difficulty_book = (Excel.Workbook)difficulty_shape.OLEFormat.Object;
        //    Excel.Worksheet difficulty_Sheet = shs.get_Item(1);

        //    difficulty_Sheet.Cells.Clear();

        //    DataTable difficulty_data = ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_difficulty;
        //    object[,] mid_data = new object[difficulty_data.Rows.Count + 1, difficulty_data.Columns.Count + 1];
        //    int in_row = 0;
        //    foreach (DataRow dr1 in difficulty_data.Rows)
        //    {
        //        int col = 0;
        //        foreach (var item in dr1.ItemArray)
        //        {
        //            mid_data[in_row, col] = item;
        //            col++;
        //        }
        //        in_row++;
        //    }

        //    difficulty_Sheet.get_Range("A1", "B" + difficulty_data.Rows.Count).Value2 = mid_data;
        //    Excel.Chart chart_difficulty = _wk.Charts.Add(oMissing, difficulty_Sheet, oMissing, oMissing);

        //    Excel.Range difficulty_chart_rng = (Excel.Range)difficulty_Sheet.Cells[1, 1];

        //    chart_difficulty.ChartWizard(difficulty_chart_rng.CurrentRegion, Excel.XlChartType.xlLine, Type.Missing, Excel.XlRowCol.xlColumns, 1, 0, false, "", "分数", "难度", "");
        //    FormatExcel(_wk, "分数", "难度");
        //    difficulty_shape = difficulty_rng.InlineShapes.AddOLEObject(ref oClassType, _wk.Name,
        //ref oMissing, ref oMissing, ref oMissing,
        //ref oMissing, ref oMissing, ref oMissing);
        //    difficulty_shape.Range.InsertCaption(oWord.CaptionLabels["图"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
        //    difficulty_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
        //    difficulty_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
        //    //difficulty_Sheet.UsedRange.CopyPicture();
        //    //difficulty_rng.PasteExcelTable(true, true, false);
        //    //difficulty_shape.ConvertToShape();
        //    difficulty_shape.Width = 375;
        //    difficulty_shape.Height = 220;
        //    Word.Range total_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    total_rng.MoveStart(Word.WdUnits.wdParagraph, 1);
        //    total_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
        //    difficulty_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    difficulty_rng.InsertParagraphAfter();
        //    ReleaseExcel(_wk, eapp);
        }
        public void insertHKSingleGroupDistChart(string title, int group_num)
        {
            DataTable dt = ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_dist;
            double[][] data = new double[dt.Rows.Count][];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                data[i] = new double[2];
                data[i][0] = Convert.ToDouble((decimal)dt.Rows[i][0]);
                data[i][1] = Convert.ToDouble((int)dt.Rows[i][1]);

            }
            double[] cuvedata = new double[2];
            cuvedata[0] = Convert.ToDouble(_sdata.group_analysis.Rows[group_num]["avg"]);
            cuvedata[1] = Convert.ToDouble(_sdata.group_analysis.Rows[group_num]["standardErr"]);
            ZedGraph.createCuveAndBar(cuvedata, data, Convert.ToDouble(_sdata.group_analysis.Rows[group_num]["max"]));
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
        //    Excel.Application eapp = new Excel.Application();
        //    eapp.Visible = false;
        //    Excel.Workbooks wk = eapp.Workbooks;
        //    Excel._Workbook _wk = wk.Add(oMissing);
        //    Excel.Sheets shs = _wk.Sheets;

        //    Word.InlineShape dist_shape;
        //    Word.Range dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;

            
            
        //    //Excel.Workbook dist_book = (Excel.Workbook)dist_shape.OLEFormat.Object;
        //    Excel.Worksheet dist_Sheet = shs.get_Item(1);

        //    dist_Sheet.Cells.Clear();

        //    DataTable dist_data = ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_dist;
        //    object[,] data = new object[dist_data.Rows.Count + 1, dist_data.Columns.Count + 1];
        //    int in_row = 0;
        //    foreach (DataRow dr1 in dist_data.Rows)
        //    {
        //        int col = 0;
        //        foreach (var item in dr1.ItemArray)
        //        {
        //            data[in_row, col] = item;
        //            col++;
        //        }
        //        in_row++;
        //    }

        //    dist_Sheet.get_Range("A1", "B" + dist_data.Rows.Count).Value2 = data;
        //    Excel.Chart chart_dist = _wk.Charts.Add(oMissing, dist_Sheet, oMissing, oMissing);

        //    Excel.Range dist_chart_rng = (Excel.Range)dist_Sheet.Cells[1, 1];

        //    chart_dist.ChartWizard(dist_chart_rng.CurrentRegion, Excel.XlChartType.xlColumnClustered, Type.Missing, Excel.XlRowCol.xlColumns, 1, 0, false, "", "分数", "人数", "");
        //    FormatExcel(_wk, "分数", "人数");
        //    dist_shape = dist_rng.InlineShapes.AddOLEObject(ref oClassType, _wk.Name,
        //ref oMissing, ref oMissing, ref oMissing,
        //ref oMissing, ref oMissing, ref oMissing);

        //    dist_shape.Range.InsertCaption(oWord.CaptionLabels["图"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
        //    dist_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
        //    dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
        //    //dist_Sheet.UsedRange.CopyPicture();
        //    dist_shape.Width = 375;
        //    dist_shape.Height = 220;
        //    //dist_rng.PasteExcelTable(true, true, false);
        //    //dist_shape.ConvertToShape();
        //    Word.Range total_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    total_rng.MoveStart(Word.WdUnits.wdParagraph, 1);
        //    total_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
        //    dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    dist_rng.InsertParagraphAfter();
        //    ReleaseExcel(_wk, eapp);
        //    Thread.Sleep(2000);
        }
        public void insertHKSingleGroupRank(string title, DataRow dr, decimal diff)
        {
            Word.Table single_total_table;
            Word.Range single_table_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            single_total_table = oDoc.Tables.Add(single_table_range, 2, 5, ref oMissing, oTrue);
            single_total_table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);

            single_table_range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            single_table_range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            single_total_table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            single_total_table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            single_total_table.Cell(1, 1).Range.Text = "优秀";
            single_total_table.Cell(1, 2).Range.Text = "良好";
            single_total_table.Cell(1, 3).Range.Text = "合格";
            single_total_table.Cell(1, 4).Range.Text = "不及格";
            single_total_table.Cell(1, 5).Range.Text = "全体";

            single_total_table.Cell(2, 1).Range.Text = string.Format("{0:F2}", dr["outstanding"]);
            single_total_table.Cell(2, 2).Range.Text = string.Format("{0:F2}", dr["good"]);
            single_total_table.Cell(2, 3).Range.Text = string.Format("{0:F2}", dr["pass"]);
            single_total_table.Cell(2, 4).Range.Text = string.Format("{0:F2}", dr["fail"]);
            single_total_table.Cell(2, 5).Range.Text = string.Format("{0:F2}", diff);

            single_total_table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            single_table_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            single_table_range.InsertParagraphAfter();
        }
        public void insertHKSingleGrouptable(string title, DataRow dr)
        {
             
            Word.Table single_total_table;
            Word.Range single_table_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            single_total_table = oDoc.Tables.Add(single_table_range, 2, 9, ref oMissing, oTrue);
            single_total_table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);

            single_table_range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            single_table_range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            single_total_table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            single_total_table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;



            single_total_table.Cell(1, 1).Range.Text = "人数";
            single_total_table.Cell(1, 2).Range.Text = "满分";
            single_total_table.Cell(1, 3).Range.Text = "最大值";
            single_total_table.Cell(1, 4).Range.Text = "最小值";
            single_total_table.Cell(1, 5).Range.Text = "平均值";
            single_total_table.Cell(1, 6).Range.Text = "标准差";
            single_total_table.Cell(1, 7).Range.Text = "差异系数";
            single_total_table.Cell(1, 8).Range.Text = "难度";
            single_total_table.Cell(1, 9).Range.Text = "区分度";

            single_total_table.Cell(2, 1).Range.Text = _sdata.total_num.ToString();
            single_total_table.Cell(2, 2).Range.Text = Convert.ToInt32(dr["fullmark"]).ToString();
            single_total_table.Cell(2, 3).Range.Text = string.Format("{0:F1}", dr["max"]);
            single_total_table.Cell(2, 4).Range.Text = string.Format("{0:F1}", dr["min"]);
            single_total_table.Cell(2, 5).Range.Text = string.Format("{0:F2}", dr["avg"]);
            single_total_table.Cell(2, 6).Range.Text = string.Format("{0:F2}", dr["standardErr"]);
            single_total_table.Cell(2, 7).Range.Text = string.Format("{0:F2}", dr["dfactor"]);
            single_total_table.Cell(2, 8).Range.Text = string.Format("{0:F2}", dr["difficulty"]);
            single_total_table.Cell(2, 9).Range.Text = string.Format("{0:F2}", dr["correlation"]);

            single_total_table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            single_table_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            single_table_range.InsertParagraphAfter();
            //oDoc.Characters.Last.InsertBreak(oParagrahbreak);
        
        }
        public void insertHKFreq(string title)
        {
            Word.Table freq_table;
            Word.Range freq_rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            freq_table = oDoc.Tables.Add(freq_rng, _sdata.frequency_dist.Rows.Count + 1, 5, ref oMissing, oTrue);
            freq_table.Rows[1].HeadingFormat = -1;
            freq_table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            freq_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
            freq_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            
            freq_table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            freq_table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

          
            freq_table.Cell(1, 1).Range.Text = "分值";
            freq_table.Cell(1, 2).Range.Text = "人数";
            freq_table.Cell(1, 3).Range.Text = "比率(%)";
            freq_table.Cell(1, 4).Range.Text = "累加人数";
            freq_table.Cell(1, 5).Range.Text = "累计比率(%)";

            for (int i = 0; i < _sdata.frequency_dist.Rows.Count; i++)
            {
                freq_table.Cell(i + 2, 1).Range.Text = string.Format("{0:F1}", _sdata.frequency_dist.Rows[i]["totalmark"]);
                freq_table.Cell(i + 2, 2).Range.Text = _sdata.frequency_dist.Rows[i]["frequency"].ToString();
                freq_table.Cell(i + 2, 3).Range.Text = string.Format("{0:F2}", _sdata.frequency_dist.Rows[i]["rate"]);
                freq_table.Cell(i + 2, 4).Range.Text = _sdata.frequency_dist.Rows[i]["accumulateFreq"].ToString();
                freq_table.Cell(i + 2, 5).Range.Text = string.Format("{0:F2}", _sdata.frequency_dist.Rows[i]["accumulateRate"]);

            }
            freq_table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            freq_rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            freq_rng.InsertParagraphAfter();
        }
        public void insertHKTotalRankTable(string title)
        {
            HK_worddata data = (HK_worddata)_sdata;
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            table = oDoc.Tables.Add(range, data.total_topic_rank.Rows.Count + 1, 5, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            table.Cell(1, 1).Range.Text = "题目或题组";
            table.Cell(1, 2).Range.Text = "优秀";
            table.Cell(1, 3).Range.Text = "良好";
            table.Cell(1, 4).Range.Text = "合格";
            table.Cell(1, 5).Range.Text = "不合格";

            for (int i = 0; i < data.total_topic_rank.Rows.Count; i++)
            {
                DataRow dr = data.total_topic_rank.Rows[i];
                table.Cell(i + 2, 1).Range.Text = dr["number"].ToString();
                table.Cell(i + 2, 2).Range.Text = string.Format("{0:F2}", dr["outstanding"]);
                table.Cell(i + 2, 3).Range.Text = string.Format("{0:F2}", dr["good"]);
                table.Cell(i + 2, 4).Range.Text = string.Format("{0:F2}", dr["pass"]);
                table.Cell(i + 2, 5).Range.Text = string.Format("{0:F2}", dr["fail"]);

            }
            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();

        }
        public void insertHKTotalAnalysisTable(string title)
        {
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            table = oDoc.Tables.Add(range, _sdata.total_analysis.Rows.Count + _sdata.group_analysis.Rows.Count + 2, 10, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            table.Cell(1, 1).Range.Text = "题目或题组";
            table.Cell(1, 2).Range.Text = "人数";
            table.Cell(1, 3).Range.Text = "满分值";
            table.Cell(1, 4).Range.Text = "最大值";
            table.Cell(1, 5).Range.Text = "最小值";
            table.Cell(1, 6).Range.Text = "平均值";
            table.Cell(1, 7).Range.Text = "标准差";
            table.Cell(1, 8).Range.Text = "差异系数";
            table.Cell(1, 9).Range.Text = "难度";
            table.Cell(1, 10).Range.Text = "区分度";
            int i;
            for (i = 0; i < _sdata.total_analysis.Rows.Count; i++)
            {
                DataRow dr = _sdata.total_analysis.Rows[i];
                table.Cell(i + 2, 1).Range.Text = dr["number"].ToString().Substring(1);
                table.Cell(i + 2, 2).Range.Text = _sdata.total_num.ToString();
                table.Cell(i + 2, 3).Range.Text = Convert.ToInt32(dr["fullmark"]).ToString();
                table.Cell(i + 2, 4).Range.Text = string.Format("{0:F1}", dr["max"]);
                table.Cell(i + 2, 5).Range.Text = string.Format("{0:F1}", dr["min"]);
                table.Cell(i + 2, 6).Range.Text = string.Format("{0:F2}", dr["avg"]);
                table.Cell(i + 2, 7).Range.Text = string.Format("{0:F2}", dr["standardErr"]);
                table.Cell(i + 2, 8).Range.Text = string.Format("{0:F2}", dr["dfactor"]);
                table.Cell(i + 2, 9).Range.Text = string.Format("{0:F2}", dr["difficulty"]);
                table.Cell(i + 2, 10).Range.Text = string.Format("{0:F2}", dr["correlation"]);
                if (Math.Abs((decimal)dr["correlation"]) != (decimal)dr["correlation"])
                    for (int k = 1; k < 11; k++)
                        table.Cell(i + 2, k).Range.Shading.BackgroundPatternColor = Word.WdColor.wdColorGray10;
            }
            table.Cell(i + 2, 1).Range.Text = "总分";
            table.Cell(i + 2, 2).Range.Text = _sdata.total_num.ToString();
            table.Cell(i + 2, 3).Range.Text = Convert.ToInt32(_sdata.fullmark).ToString();
            table.Cell(i + 2, 4).Range.Text = string.Format("{0:F1}", _sdata.max);
            table.Cell(i + 2, 5).Range.Text = string.Format("{0:F1}", _sdata.min);
            table.Cell(i + 2, 6).Range.Text = string.Format("{0:F2}", _sdata.avg);
            table.Cell(i + 2, 7).Range.Text = string.Format("{0:F2}", _sdata.stDev);
            table.Cell(i + 2, 8).Range.Text = string.Format("{0:F2}", _sdata.Dfactor);
            table.Cell(i + 2, 9).Range.Text = string.Format("{0:F2}", _sdata.difficulty);
            table.Cell(i + 2, 10).Range.Text = "1.00";

            for (int j = 0; j < _sdata.group_analysis.Rows.Count; j++)
            {
                DataRow dr = _sdata.group_analysis.Rows[j];
                table.Cell(j + i + 3, 1).Range.Text = dr["number"].ToString();
                table.Cell(j + i + 3, 2).Range.Text = _sdata.total_num.ToString();
                table.Cell(j + i + 3, 3).Range.Text = Convert.ToInt32(dr["fullmark"]).ToString();
                table.Cell(j + i + 3, 4).Range.Text = string.Format("{0:F1}", dr["max"]);
                table.Cell(j + i + 3, 5).Range.Text = string.Format("{0:F1}", dr["min"]);
                table.Cell(j + i + 3, 6).Range.Text = string.Format("{0:F2}", dr["avg"]);
                table.Cell(j + i + 3, 7).Range.Text = string.Format("{0:F2}", dr["standardErr"]);
                table.Cell(j + i + 3, 8).Range.Text = string.Format("{0:F2}", dr["dfactor"]);
                table.Cell(j + i + 3, 9).Range.Text = string.Format("{0:F2}", dr["difficulty"]);
                table.Cell(j + i + 3, 10).Range.Text = string.Format("{0:F2}", dr["correlation"]);
                if (Math.Abs((decimal)dr["correlation"]) != (decimal)dr["correlation"])
                    for (int k = 1; k < 11; k++)
                        table.Cell(j + i + 3, k).Range.Shading.BackgroundPatternColor = Word.WdColor.wdColorGray10;
            }

            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();
        }
        public void insertHKTotalRank(string title)
        {
            HK_worddata data = (HK_worddata)_sdata;
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            table = oDoc.Tables.Add(range, 6, 7, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            table.Cell(1, 1).Range.Text = "等级";
            table.Cell(1, 2).Range.Text = "人数";
            table.Cell(1, 3).Range.Text = "比率";
            table.Cell(1, 4).Range.Text = "平均值";
            table.Cell(1, 5).Range.Text = "标准差";
            table.Cell(1, 6).Range.Text = "差异系数";
            table.Cell(1, 7).Range.Text = "得分率";

            for (int i = 0; i < data.total.Rows.Count; i++)
            {
                table.Cell(i + 2, 1).Range.Text = data.total.Rows[i][0].ToString().Trim();
                table.Cell(i + 2, 2).Range.Text = data.total.Rows[i][1].ToString().Trim();
                table.Cell(i + 2, 3).Range.Text = string.Format("{0:F2}", data.total.Rows[i][2]);
                table.Cell(i + 2, 4).Range.Text = string.Format("{0:F2}", data.total.Rows[i][3]);
                table.Cell(i + 2, 5).Range.Text = string.Format("{0:F2}", data.total.Rows[i][4]);
                table.Cell(i + 2, 6).Range.Text = string.Format("{0:F2}", data.total.Rows[i][5]);
                table.Cell(i + 2, 7).Range.Text = string.Format("{0:F2}", data.total.Rows[i][6]);
            }
            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();

        }
        
        public void insertHKTotalTable(string title)
        {
            HK_worddata data = (HK_worddata)_sdata;
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            table = oDoc.Tables.Add(range, 4, 4, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            table.Cell(1, 1).Range.Text = "人数";
            table.Cell(1, 2).Range.Text = "满分值";
            table.Cell(1, 3).Range.Text = "最大值";
            table.Cell(1, 4).Range.Text = "最小值";
            table.Cell(3, 1).Range.Text = "平均值";
            table.Cell(3, 2).Range.Text = "标准差";
            table.Cell(3, 3).Range.Text = "差异系数";
            table.Cell(3, 4).Range.Text = "难度";

            table.Cell(2, 1).Range.Text = data.total_num.ToString();
            table.Cell(2, 2).Range.Text = Convert.ToInt32(data.fullmark).ToString();
            table.Cell(2, 3).Range.Text = string.Format("{0:F1}", data.max);
            table.Cell(2, 4).Range.Text = string.Format("{0:F1}", data.min);
            table.Cell(4, 1).Range.Text = string.Format("{0:F2}", data.avg);
            table.Cell(4, 2).Range.Text = string.Format("{0:F2}", data.stDev);
            table.Cell(4, 3).Range.Text = string.Format("{0:F2}", data.Dfactor);
            table.Cell(4, 4).Range.Text = string.Format("{0:F2}", data.difficulty);

            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();
        }
        public void insertTotalTable(WordData sdata)
        {
            Word.Table Total_Table;
            Word.Range total_rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            Total_Table = oDoc.Tables.Add(total_rng, 4, 7, ref oMissing, oTrue);
            object Total_title = "    总分分析表";
            Total_Table.Range.InsertCaption(oWord.CaptionLabels["表"], Total_title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            total_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
            total_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //Total_Table.Range.set_Style(ref TableContent);

            //oWord.Selection.Cells.VerticalAlignment = Word.WdCellVerticalAlignment.wdCellAlignVerticalCenter;
            //oWord.Selection.ParagraphFormat.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //Total_Table.Range.ParagraphFormat.Space1();
            Total_Table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            Total_Table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            //Total_Table.Range.Paragraphs.OutlineLevel = Word.WdOutlineLevel.wdOutlineLevelBodyText;
            //Total_Table.Range.Font.Size = 10;
            //Total_Table.Range.Font.Name = "黑体";
            Total_Table.Cell(1, 1).Range.Text = "总人数";
            Total_Table.Cell(1, 2).Range.Text = "满分值";
            Total_Table.Cell(1, 3).Range.Text = "最大值";
            Total_Table.Cell(1, 4).Range.Text = "最小值";
            Total_Table.Cell(1, 5).Range.Text = "平均值";
            Total_Table.Cell(1, 6).Range.Text = "标准差";
            Total_Table.Cell(1, 7).Range.Text = "差异系数";

            Total_Table.Cell(2, 1).Range.Text = sdata.total_num.ToString();
            Total_Table.Cell(2, 2).Range.Text = Convert.ToInt32(sdata.fullmark).ToString();
            Total_Table.Cell(2, 3).Range.Text = string.Format("{0:F1}", sdata.max);
            Total_Table.Cell(2, 4).Range.Text = string.Format("{0:F1}", sdata.min);
            Total_Table.Cell(2, 5).Range.Text = string.Format("{0:F2}", sdata.avg);
            Total_Table.Cell(2, 6).Range.Text = string.Format("{0:F2}", sdata.stDev);
            Total_Table.Cell(2, 7).Range.Text = string.Format("{0:F2}", sdata.Dfactor);

            Total_Table.Cell(3, 1).Range.Text = "难度";
            Total_Table.Cell(3, 2).Range.Text = "alpha系数";
            Total_Table.Cell(3, 3).Range.Text = "标准误";
            Total_Table.Cell(3, 4).Range.Text = "中数";
            Total_Table.Cell(3, 5).Range.Text = "众数";
            Total_Table.Cell(3, 6).Range.Text = "偏度";
            Total_Table.Cell(3, 7).Range.Text = "峰度";

            Total_Table.Cell(4, 1).Range.Text = string.Format("{0:F2}", sdata.difficulty);
            Total_Table.Cell(4, 2).Range.Text = string.Format("{0:F2}", sdata.alfa);
            Total_Table.Cell(4, 3).Range.Text = string.Format("{0:F2}", sdata.standardErr);
            Total_Table.Cell(4, 4).Range.Text = string.Format("{0:F2}", sdata.mean);
            Total_Table.Cell(4, 5).Range.Text = string.Format("{0:F1}", sdata.mode);
            Total_Table.Cell(4, 6).Range.Text = string.Format("{0:F2}", sdata.skewness);
            Total_Table.Cell(4, 7).Range.Text = string.Format("{0:F2}", sdata.kertosis);

            Total_Table.Range.Select();
            oWord.Selection.set_Style(ref TableContent2);

            total_rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            total_rng.InsertParagraphAfter();
            //object WdLine = Microsoft.Office.Interop.Word.WdUnits.wdLine;
            //oWord.Selection.MoveDown(WdLine, 4, oMissing);
            //oWord.Selection.TypeParagraph();
            //oDoc.Characters.Last.InsertBreak(oParagrahbreak);
        }
        public void insertTotalGroupTable(string title, WordData sdata, int rowcount)
        {
            Word.Table groups_table;
            Word.Range groups_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            groups_table = oDoc.Tables.Add(groups_range, rowcount, 10, ref oMissing, oTrue);
            groups_table.Rows[1].HeadingFormat = -1;
            groups_table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            groups_range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            groups_range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //groups_table.Range.set_Style(ref TableContent2);
            //groups_table.Range.Select();
            //oWord.Selection.Cells.VerticalAlignment = Word.WdCellVerticalAlignment.wdCellAlignVerticalCenter;
            //oWord.Selection.ParagraphFormat.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //groups_table.Range.ParagraphFormat.Space1();
            groups_table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            groups_table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            //groups_table.Range.Paragraphs.OutlineLevel = Word.WdOutlineLevel.wdOutlineLevelBodyText;
            //groups_table.Range.Font.Size = 10;
            //groups_table.Range.Font.Name = "黑体";

            groups_table.Cell(1, 1).Range.Text = "题目";
            groups_table.Cell(1, 2).Range.Text = "满分值";
            groups_table.Cell(1, 3).Range.Text = "最大值";
            groups_table.Cell(1, 4).Range.Text = "最小值";
            groups_table.Cell(1, 5).Range.Text = "平均值";
            groups_table.Cell(1, 6).Range.Text = "标准差";
            groups_table.Cell(1, 7).Range.Text = "差异系数";
            groups_table.Cell(1, 8).Range.Text = "难度";
            groups_table.Cell(1, 9).Range.Text = "相关系数";
            groups_table.Cell(1, 10).Range.Text = "鉴别指数";

            for (int i = 0; i < rowcount - 1; i++)
            {
                groups_table.Cell(i + 2, 1).Range.Text = sdata.group_analysis.Rows[i]["number"].ToString();
                groups_table.Cell(i + 2, 2).Range.Text = Convert.ToInt32(sdata.group_analysis.Rows[i]["fullmark"]).ToString();
                groups_table.Cell(i + 2, 3).Range.Text = string.Format("{0:F1}", sdata.group_analysis.Rows[i]["max"]);
                groups_table.Cell(i + 2, 4).Range.Text = string.Format("{0:F1}", sdata.group_analysis.Rows[i]["min"]);
                groups_table.Cell(i + 2, 5).Range.Text = string.Format("{0:F2}", sdata.group_analysis.Rows[i]["avg"]);
                groups_table.Cell(i + 2, 6).Range.Text = string.Format("{0:F2}", sdata.group_analysis.Rows[i]["standardErr"]);
                groups_table.Cell(i + 2, 7).Range.Text = string.Format("{0:F2}", sdata.group_analysis.Rows[i]["dfactor"]);
                groups_table.Cell(i + 2, 8).Range.Text = string.Format("{0:F2}", sdata.group_analysis.Rows[i]["difficulty"]);
                groups_table.Cell(i + 2, 9).Range.Text = string.Format("{0:F2}", sdata.group_analysis.Rows[i]["correlation"]);
                groups_table.Cell(i + 2, 10).Range.Text = string.Format("{0:F2}", sdata.group_analysis.Rows[i]["discriminant"]);
                if (Math.Abs((decimal)sdata.group_analysis.Rows[i]["correlation"]) != (decimal)sdata.group_analysis.Rows[i]["correlation"] ||
                    Math.Abs((decimal)sdata.group_analysis.Rows[i]["discriminant"]) != (decimal)sdata.group_analysis.Rows[i]["discriminant"])
                    for (int j = 1; j < 11; j++)
                        groups_table.Cell(i + 2, j).Range.Shading.BackgroundPatternColor = Word.WdColor.wdColorGray10;
            }
            groups_table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            groups_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            groups_range.InsertParagraphAfter();
        }
        public void insertTotalFreqTable(WordData sdata)
        {
            Word.Table freq_table;
            Word.Range freq_rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            freq_table = oDoc.Tables.Add(freq_rng, sdata.frequency_dist.Rows.Count + 1, 5, ref oMissing, oTrue);
            freq_table.Rows[1].HeadingFormat = -1;
            object freq_title = "    总分人数分布表";
            freq_table.Range.InsertCaption(oWord.CaptionLabels["表"], freq_title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            freq_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
            freq_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //freq_table.Range.set_Style(ref TableContent2);

            //freq_table.Range.Select();
            //oWord.Selection.Cells.VerticalAlignment = Word.WdCellVerticalAlignment.wdCellAlignVerticalCenter;
            //oWord.Selection.ParagraphFormat.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //freq_table.Range.ParagraphFormat.Space1();
            freq_table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            freq_table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            //freq_table.Range.Paragraphs.OutlineLevel = Word.WdOutlineLevel.wdOutlineLevelBodyText;
            //freq_table.Range.Font.Size = 10;
            //freq_table.Range.Font.Name = "黑体";

            freq_table.Cell(1, 1).Range.Text = "分值";
            freq_table.Cell(1, 2).Range.Text = "人数";
            freq_table.Cell(1, 3).Range.Text = "比率(%)";
            freq_table.Cell(1, 4).Range.Text = "累计人数";
            freq_table.Cell(1, 5).Range.Text = "累计频率（%）";

            for (int i = 0; i < sdata.frequency_dist.Rows.Count; i++)
            {
                freq_table.Cell(i + 2, 1).Range.Text = string.Format("{0:F1}", sdata.frequency_dist.Rows[i]["totalmark"]);
                freq_table.Cell(i + 2, 2).Range.Text = sdata.frequency_dist.Rows[i]["frequency"].ToString();
                freq_table.Cell(i + 2, 3).Range.Text = string.Format("{0:F2}", sdata.frequency_dist.Rows[i]["rate"]);
                freq_table.Cell(i + 2, 4).Range.Text = sdata.frequency_dist.Rows[i]["accumulateFreq"].ToString();
                freq_table.Cell(i + 2, 5).Range.Text = string.Format("{0:F2}", sdata.frequency_dist.Rows[i]["accumulateRate"]);

            }
            freq_table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            freq_rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            freq_rng.InsertParagraphAfter();
        }
        public void insertGroupTotalTable(DataRow group_dr)
        {
            Word.Table single_total_table;
            Word.Range single_table_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            single_total_table = oDoc.Tables.Add(single_table_range, 2, 9, ref oMissing, oTrue);
            object single_table_title = "    " + group_dr["number"].ToString().Trim() + "总分分布表";
            single_total_table.Range.InsertCaption(oWord.CaptionLabels["表"], single_table_title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            single_table_range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            single_table_range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;


            single_total_table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            single_total_table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;



            single_total_table.Cell(1, 1).Range.Text = "满分值";
            single_total_table.Cell(1, 2).Range.Text = "最大值";
            single_total_table.Cell(1, 3).Range.Text = "最小值";
            single_total_table.Cell(1, 4).Range.Text = "平均值";
            single_total_table.Cell(1, 5).Range.Text = "标准差";
            single_total_table.Cell(1, 6).Range.Text = "差异系数";
            single_total_table.Cell(1, 7).Range.Text = "难度";
            single_total_table.Cell(1, 8).Range.Text = "相关系数";
            single_total_table.Cell(1, 9).Range.Text = "鉴别指数";

            single_total_table.Cell(2, 1).Range.Text = Convert.ToInt32(group_dr["fullmark"]).ToString();
            single_total_table.Cell(2, 2).Range.Text = string.Format("{0:F1}", group_dr["max"]);
            single_total_table.Cell(2, 3).Range.Text = string.Format("{0:F1}", group_dr["min"]);
            single_total_table.Cell(2, 4).Range.Text = string.Format("{0:F2}", group_dr["avg"]);
            single_total_table.Cell(2, 5).Range.Text = string.Format("{0:F2}", group_dr["standardErr"]);
            single_total_table.Cell(2, 6).Range.Text = string.Format("{0:F2}", group_dr["dfactor"]);
            single_total_table.Cell(2, 7).Range.Text = string.Format("{0:F2}", group_dr["difficulty"]);
            single_total_table.Cell(2, 8).Range.Text = string.Format("{0:F2}", group_dr["correlation"]);
            single_total_table.Cell(2, 9).Range.Text = string.Format("{0:F2}", group_dr["discriminant"]);

            single_total_table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            single_table_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            single_table_range.InsertParagraphAfter();
        }
        public void insertGroupSingleAnalysis(string title, DataTable single_group_dt)
        {
            Word.Table single_group_analysis;
            Word.Range single_group_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;

            //DataTable single_group_dt = ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_detail;
            single_group_analysis = oDoc.Tables.Add(single_group_rng, single_group_dt.Rows.Count + 1, single_group_dt.Columns.Count, ref oMissing, oTrue);

            single_group_analysis.Rows[1].HeadingFormat = -1;
            //object single_group_title = group_dr["number"].ToString().Trim() + "分组分析表";
            single_group_analysis.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            single_group_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
            single_group_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //single_group_analysis.Range.set_Style(TableContent2);
            //single_group_analysis.Range.Select();
            //oWord.Selection.Cells.VerticalAlignment = Word.WdCellVerticalAlignment.wdCellAlignVerticalCenter;
            //oWord.Selection.ParagraphFormat.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //single_group_analysis.Range.ParagraphFormat.Space1();
            single_group_analysis.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            single_group_analysis.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            //single_group_analysis.Range.Paragraphs.OutlineLevel = Word.WdOutlineLevel.wdOutlineLevelBodyText;
            //single_group_analysis.Range.Font.Size = 10;
            //single_group_analysis.Range.Font.Name = "黑体";
            int i;
            int j = 0;
            for (i = 0; i < single_group_dt.Columns.Count; i++)
            {
                if (single_group_dt.Columns[i].ColumnName.Trim().Equals("mark"))
                    single_group_analysis.Cell(1, i + 1).Range.Text = "分值";
                else if (single_group_dt.Columns[i].ColumnName.Trim().Equals("rate"))
                    single_group_analysis.Cell(1, i + 1).Range.Text = "比率(%)";
                else if (single_group_dt.Columns[i].ColumnName.Trim().Equals("avg"))
                    single_group_analysis.Cell(1, i + 1).Range.Text = "平均值";
                else if (single_group_dt.Columns[i].ColumnName.Trim().Equals("frequency"))
                    single_group_analysis.Cell(1, i + 1).Range.Text = "人数";
                else
                    single_group_analysis.Cell(1, i + 1).Range.Text = single_group_dt.Columns[i].ColumnName;
            }
            for (i = 0; i < single_group_dt.Rows.Count; i++)
            {

                for (j = 0; j < single_group_dt.Columns.Count; j++)
                {
                    if (i == single_group_dt.Rows.Count - 1)
                    {
                        if (single_group_dt.Columns[j].ColumnName.Trim().Equals("rate") ||
                        single_group_dt.Columns[j].ColumnName.Trim().Equals("avg") || single_group_dt.Columns[j].ColumnName.Trim().Equals("frequency"))
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = "-";
                        else if (single_group_dt.Columns[j].ColumnName.Trim().Equals("mark"))
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = single_group_dt.Rows[i][j].ToString().Trim();

                        else
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = string.Format("{0:F2}", single_group_dt.Rows[i][j]);
                    }
                    else
                    {
                        if (single_group_dt.Columns[j].ColumnName.Trim().Equals("rate") ||
                            single_group_dt.Columns[j].ColumnName.Trim().Equals("avg"))
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = string.Format("{0:F2}", single_group_dt.Rows[i][j]);
                        else if (single_group_dt.Columns[j].ColumnName.Trim().Equals("mark"))
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = single_group_dt.Rows[i][j].ToString().Trim();
                        else if (single_group_dt.Columns[j].ColumnName.Trim().Equals("frequency"))
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = single_group_dt.Rows[i][j].ToString();
                        else
                            single_group_analysis.Cell(i + 2, j + 1).Range.Text = Convert.ToInt32(single_group_dt.Rows[i][j]).ToString().Trim();
                    }



                }
            }
            single_group_analysis.Select();
            oWord.Selection.set_Style(ref TableContent2);
            Word.Range single_table_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            single_table_range.InsertParagraphAfter();
        }
        public void creating_word()
        {
            string subject = Utils.subject;
            object filepath;
            if(isZonghe)
                filepath = @Utils.CurrentDirectory + @"\template2.dotx";
            else
                filepath = @Utils.CurrentDirectory + @"\template.dotx";
            //Start Word and create a new document.
            _addr = Utils.save_address + @"\" + Utils.subject + ".docx";
            oWord = new Word.Application();

            oWord.Visible = Utils.isVisible;
            oDoc = oWord.Documents.Add(ref filepath, ref oMissing,
            ref oMissing, ref oMissing);
            Utils.WriteFrontPage(oDoc);
            
            object oPageBreak = Microsoft.Office.Interop.Word.WdBreakType.wdPageBreak;
            
            //oDoc.Characters.Last.InsertBreak(oPageBreak);
            if (isZonghe)
            {
                insertText(ExamTitle0, "整体统计分析");
                insertText(ExamTitle1, "总体分析");
                insertTotalTable(_ZH_data);
                insertTotalChart("    总分分布曲线图", _ZH_data);
                insertTotalGroupTable("    科目整体分析表", _ZH_data, 4);
                insertTotalFreqTable(_ZH_data);
                insertText(ExamTitle1, "题组分析");
                List<string> keys = new List<string>(_ZH_data.groups_group.Keys);
                int zh_group_count = 3;
                for (int i = 1; i < _ZH_data.groups_group.Count; i++)
                {
                    string key = keys[i];
                    insertText(ExamTitle2, key);
                    List<string> groups = _ZH_data.groups_group[key];
                    foreach (string group in groups)
                    {
                        if (group.Equals("totalmark"))
                            continue;
                        WordData.group_data group_dt = (WordData.group_data)_ZH_data.single_group_analysis[zh_group_count];
                        DataRow group_dr = _ZH_data.group_analysis.Rows[zh_group_count];
                        insertText(ExamTitle3, group);
                        insertTH(_ZH_data._groups_ans.Rows[zh_group_count]["th"].ToString().Trim());
                        insertGroupTotalTable(group_dr);
                        insertGroupDistChart("    " + group + "分数分布表", group_dt.group_dist);
                        insertGroupDiffChart("    " + group + "难度曲线图", group_dt.group_difficulty);
                        insertGroupSingleAnalysis("    " + group + "分组分析表", group_dt.group_detail);
                        zh_group_count++;
                        oDoc.Characters.Last.InsertBreak(oPagebreak);
                    }
                }
                //for (int i = 3; i < _ZH_data.single_group_analysis.Count; i++)
                //{
                //    WordData.group_data group_dt = (WordData.group_data)_ZH_data.single_group_analysis[i];
                //    DataRow group_dr = _ZH_data.group_analysis.Rows[i];
                //    insertText(ExamTitle3, _ZH_data.group_analysis.Rows[i]["number"].ToString());
                //    insertText(ExamBodyText, "本题组包含:" + _ZH_data._groups_ans.Rows[i]["th"].ToString().Trim());
                //    insertGroupTotalTable(group_dr);
                //    insertGroupDistChart(group_dr["number"].ToString().Trim() + "分数分布表", group_dt.group_dist);
                //    insertGroupDiffChart(group_dr["number"].ToString().Trim() + "难度曲线图", group_dt.group_difficulty);
                //    insertGroupSingleAnalysis(group_dr["number"].ToString().Trim() + "分组分析表", group_dt.group_detail);
                //}
                insertText(ExamTitle0, Utils.subject.Substring(3) + "统计分析");
            }
            
            Word.Range first = oDoc.Paragraphs.Add(ref oMissing).Range;
            first.set_Style(ExamTitle1);
            first.InsertBefore("总体分析\n");
            
            
            
            oDoc.Characters.Last.Select();
            oWord.Selection.HomeKey(Word.WdUnits.wdLine, oMissing);
            oWord.Selection.Delete(Word.WdUnits.wdCharacter, oMissing);
            oWord.Selection.Range.set_Style(ExamBodyText);
            
            //oPara2.Range.ListFormat.ApplyListTemplate(listTemp, ref bContinuousPrev, ref applyTo, ref defaultListBehaviour);
            //oPara2.Format.SpaceAfter = 6;
            //oPara2.Range.InsertParagraphAfter();


            insertTotalTable(_sdata);

            ///////////////////////////////////////////////////////////

            insertTotalChart("    总分分布曲线图", _sdata);


            insertTotalDifficultyChart();
           
            //oDoc.Characters.Last.InsertBreak(oParagrahbreak);



            Word.Table topic_table;
            Word.Range topic_Rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            topic_table = oDoc.Tables.Add(topic_Rng, _sdata.total_analysis.Rows.Count + 1, 10, ref oMissing, oTrue);
            topic_table.Rows[1].HeadingFormat = -1;
            object topic_title = "    题目整体分析表";
            topic_table.Range.InsertCaption(oWord.CaptionLabels["表"], topic_title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            topic_Rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
            topic_Rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            
            //topic_table.Range.Select();
            //oWord.Selection.Cells.VerticalAlignment = Word.WdCellVerticalAlignment.wdCellAlignVerticalCenter;
            //oWord.Selection.ParagraphFormat.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            //topic_table.Range.ParagraphFormat.Space1();
            topic_table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            topic_table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            //topic_table.Range.Paragraphs.OutlineLevel = Word.WdOutlineLevel.wdOutlineLevelBodyText;
            //topic_table.Range.Font.Size = 10;
            //topic_table.Range.Font.Name = "黑体";

            topic_table.Cell(1, 1).Range.Text = "题目";
            topic_table.Cell(1, 2).Range.Text = "满分值";
            topic_table.Cell(1, 3).Range.Text = "最大值";
            topic_table.Cell(1, 4).Range.Text = "最小值";
            topic_table.Cell(1, 5).Range.Text = "平均值";
            topic_table.Cell(1, 6).Range.Text = "标准差";
            topic_table.Cell(1, 7).Range.Text = "差异系数";
            topic_table.Cell(1, 8).Range.Text = "难度";
            topic_table.Cell(1, 9).Range.Text = "相关系数";
            topic_table.Cell(1, 10).Range.Text = "鉴别指数";
            
            for (int i = 0; i < _sdata.total_analysis.Rows.Count; i++)
            {
                topic_table.Cell(i + 2, 1).Range.Text = _sdata.total_analysis.Rows[i]["number"].ToString().Substring(1);
                topic_table.Cell(i + 2, 2).Range.Text = Convert.ToInt32(_sdata.total_analysis.Rows[i]["fullmark"]).ToString();
                topic_table.Cell(i + 2, 3).Range.Text = string.Format("{0:F1}", _sdata.total_analysis.Rows[i]["max"]);
                topic_table.Cell(i + 2, 4).Range.Text = string.Format("{0:F1}", _sdata.total_analysis.Rows[i]["min"]);
                topic_table.Cell(i + 2, 5).Range.Text = string.Format("{0:F2}", _sdata.total_analysis.Rows[i]["avg"]);
                topic_table.Cell(i + 2, 6).Range.Text = string.Format("{0:F2}", _sdata.total_analysis.Rows[i]["standardErr"]);
                topic_table.Cell(i + 2, 7).Range.Text = string.Format("{0:F2}", _sdata.total_analysis.Rows[i]["dfactor"]);
                topic_table.Cell(i + 2, 8).Range.Text = string.Format("{0:F2}", _sdata.total_analysis.Rows[i]["difficulty"]);
                topic_table.Cell(i + 2, 9).Range.Text = string.Format("{0:F2}", _sdata.total_analysis.Rows[i]["correlation"]);
                topic_table.Cell(i + 2, 10).Range.Text = string.Format("{0:F2}", _sdata.total_analysis.Rows[i]["discriminant"]);
                if (Math.Abs((decimal)_sdata.total_analysis.Rows[i]["correlation"]) != (decimal)_sdata.total_analysis.Rows[i]["correlation"] ||
                    Math.Abs((decimal)_sdata.total_analysis.Rows[i]["discriminant"]) != (decimal)_sdata.total_analysis.Rows[i]["discriminant"])
                    for (int j = 1; j < 11; j++)
                        topic_table.Cell(i + 2, j).Range.Shading.BackgroundPatternColor = Word.WdColor.wdColorGray10;
            }
            //oWord.Selection.MoveEnd();
            //oWord.Selection.MoveDown(WdLine, _sdata.total_analysis.Rows.Count + 1, oMissing);
            //oWord.Selection.InsertParagraphAfter();

            //topic_table.Range.set_Style(ref TableContent2);
            topic_table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            topic_Rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            topic_Rng.InsertParagraphAfter();
            //oDoc.Characters.Last.InsertBreak(oParagrahbreak);


            insertTotalGroupTable("    题组整体分析表", _sdata, _sdata.group_analysis.Rows.Count + 1);
            
            //oDoc.Characters.Last.InsertBreak(oParagrahbreak);


            insertTotalFreqTable(_sdata);
            //oDoc.Characters.Last.InsertBreak(oPageBreak);

            oDoc.Characters.Last.InsertBreak(oPagebreak);
            first = oDoc.Paragraphs.Add(ref oMissing).Range;
            first.set_Style(ExamTitle1);
            first.InsertBefore("题组分析\n");



            oDoc.Characters.Last.Select();
            oWord.Selection.HomeKey(Word.WdUnits.wdLine, oMissing);
            oWord.Selection.Delete(Word.WdUnits.wdCharacter, oMissing);
            oWord.Selection.Range.set_Style(ExamBodyText);

            int group_num = 0;
            int x;
            foreach (string key in _sdata.groups_group.Keys)
            {
                insertText(ExamTitle2, key);
                List<string> groups = _sdata.groups_group[key];
                foreach (string group in groups)
                {
                    if(group.Equals("totalmark"))
                        continue;
                    if(group.Equals("单选题"))
                        x = 1;
                    insertText(ExamTitle3, group);
                    insertTH(_sdata._groups_ans.Rows.Find(group)["th"].ToString().Trim());
                    DataRow group_dr = _sdata.group_analysis.Rows.Find(group);
                    
                    insertGroupTotalTable(group_dr);
                    insertGroupDistChart("    " + group + "分数分布表", ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_dist);
                    insertGroupDiffChart("    " + group + "难度曲线图", ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_difficulty);
                    insertGroupSingleAnalysis("    " + group + "分组分析表", ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_detail);
                    group_num++;
                    oDoc.Characters.Last.InsertBreak(oPagebreak);
                }
            }

            //foreach (DataRow group_dr in _sdata.group_analysis.Rows)
            //{
                
            //    first = oDoc.Paragraphs.Add(ref oMissing).Range;
            //    first.set_Style(ExamTitle2);
            //    first.InsertBefore(group_dr["number"].ToString().Trim() + "\n");

            //    oDoc.Characters.Last.Select();
            //    oWord.Selection.HomeKey(Word.WdUnits.wdLine, oMissing);
            //    oWord.Selection.Delete(Word.WdUnits.wdCharacter, oMissing);
            //    oWord.Selection.Range.set_Style(ExamBodyText);
            //    first = oDoc.Paragraphs.Add(ref oMissing).Range;

            //    first.InsertBefore("本题组包含" + _sdata._groups_ans.Rows[group_num]["th"].ToString().Trim() + "\n");

            //    insertGroupTotalTable(group_dr);
            //    insertGroupDistChart(group_dr["number"].ToString().Trim() + "分数分布表", ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_dist);
            //    insertGroupDiffChart(group_dr["number"].ToString().Trim() + "难度曲线图", ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_difficulty);
            //    insertGroupSingleAnalysis(group_dr["number"].ToString().Trim() + "分组分析表", ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_detail);
                
            //    group_num++;

            //}
            #region single topic analysis

            insertText(ExamTitle1, "题目分析");

            int topic_num = 0;
            foreach (DataRow dr in _sdata.total_analysis.Rows)
            {
                insertText(ExamTitle3, "第" + dr["number"].ToString().Trim().Substring(1) + "题");
                insertTotalTable("    " + "第" + dr["number"].ToString().Trim().Substring(1) + "题分析表", dr);
                insertChart("    " + "第" + dr["number"].ToString().Trim().Substring(1) + "题难度曲线图", ((WordData.single_data)_sdata.single_topic_analysis[topic_num]).single_difficulty, "分数", "难度", Excel.XlChartType.xlXYScatterSmooth);
                insertMultipleChart("    " + "第" + dr["number"].ToString().Trim().Substring(1) + "题分组难度曲线图", ((WordData.single_data)_sdata.single_topic_analysis[topic_num]).single_dist, "分组", "难度", Excel.XlChartType.xlLineMarkers);
                insertGroupTable("    " + "第" + dr["number"].ToString().Trim().Substring(1) + "题分组分析表", ((WordData.single_data)_sdata.single_topic_analysis[topic_num]).single_detail, ((WordData.single_data)_sdata.single_topic_analysis[topic_num]).stype);
                topic_num++;
                oDoc.Characters.Last.InsertBreak(oPagebreak);
            }

            insertText(ExamTitle1, "相关分析");
            group_num = 0;
            foreach (string key in _sdata.groups_group.Keys)
            {
                insertText(ExamTitle3, key);
                insertCorTable(key, _sdata.group_cor[group_num]);
                group_num++;
            }
            #endregion
            foreach(Word.TableOfContents table in oDoc.TablesOfContents)
                table.Update();
            Utils.Save(oDoc, oWord);
            

        }
        public void insertCorTable(string title, DataTable dt)
        {
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            table = oDoc.Tables.Add(range, dt.Rows.Count + 1, dt.Columns.Count, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title + "相关分析表", oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            for (int i = 1; i < dt.Columns.Count; i++)
            {
                if (dt.Columns[i].ColumnName.Equals("totalmark"))
                    table.Cell(1, i + 1).Range.Text = "总分";
                else
                    table.Cell(1, i + 1).Range.Text = dt.Columns[i].ColumnName;
            }
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                for (int j = 0; j < dt.Columns.Count; j++)
                {

                    if (j == 0)
                    {
                        if (i == dt.Rows.Count - 1)
                            table.Cell(i + 2, j + 1).Range.Text = "总分";
                        else
                            table.Cell(i + 2, j + 1).Range.Text = dt.Rows[i][j].ToString();
                    }
                    else if ((decimal)dt.Rows[i][j] == 1)
                        table.Cell(i + 2, j + 1).Range.Text = "-";
                    else
                    {
                        table.Cell(i + 2, j + 1).Range.Text = string.Format("{0:F4}", dt.Rows[i][j]);
                    }
                }
            }

            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();

        }
        public void insertGroupDiffChart(string title, DataTable difficulty_data)
        {
            DataTable dt = difficulty_data;
            double[][] data = new double[dt.Rows.Count][];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                data[i] = new double[2];
                data[i][0] = Convert.ToDouble((decimal)dt.Rows[i][0]);
                data[i][1] = Convert.ToDouble((decimal)dt.Rows[i][1]);

            }

            ZedGraph.createDiffCuve(data, Convert.ToDouble(dt.Compute("Min([" + dt.Columns[0].ColumnName + "])", "")), Convert.ToDouble(dt.Compute("Max([" + dt.Columns[0].ColumnName + "])", "")));
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
        //    Excel.Application eapp = new Excel.Application();
        //    eapp.Visible = false;
        //    Excel.Workbooks wk = eapp.Workbooks;
        //    Excel._Workbook _wk = wk.Add(oMissing);
        //    Excel.Sheets shs = _wk.Sheets;

        //    Word.InlineShape difficulty_shape;
        //    Word.Range difficulty_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;

            
        //    //Excel.Workbook difficulty_book = (Excel.Workbook)difficulty_shape.OLEFormat.Object;
        //    Excel.Worksheet difficulty_Sheet = shs.get_Item(1);

        //    difficulty_Sheet.Cells.Clear();

        //    //DataTable difficulty_data = ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_difficulty;
        //    object[,] mid_data = new object[difficulty_data.Rows.Count + 1, difficulty_data.Columns.Count + 1];
        //    int in_row = 0;
        //    foreach (DataRow dr1 in difficulty_data.Rows)
        //    {
        //        int col = 0;
        //        foreach (var item in dr1.ItemArray)
        //        {
        //            mid_data[in_row, col] = item;
        //            col++;
        //        }
        //        in_row++;
        //    }

        //    difficulty_Sheet.get_Range("A1", "B" + difficulty_data.Rows.Count).Value2 = mid_data;
        //    Excel.Chart chart_difficulty = _wk.Charts.Add(oMissing, difficulty_Sheet, oMissing, oMissing);

        //    Excel.Range difficulty_chart_rng = (Excel.Range)difficulty_Sheet.Cells[1, 1];

        //    chart_difficulty.ChartWizard(difficulty_chart_rng.CurrentRegion, Excel.XlChartType.xlLine, Type.Missing, Excel.XlRowCol.xlColumns, 1, 0, false, "", "分数", "难度", "");
        //    FormatExcel(_wk, "分数", "难度");

        //    difficulty_shape = difficulty_rng.InlineShapes.AddOLEObject(ref oClassType, _wk.Name,
        //ref oMissing, ref oMissing, ref oMissing,
        //ref oMissing, ref oMissing, ref oMissing);
            
        //    difficulty_shape.Range.InsertCaption(oWord.CaptionLabels["图"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
        //    difficulty_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
        //    difficulty_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            

        //    //difficulty_Sheet.UsedRange.CopyPicture();
        //    //difficulty_rng.PasteExcelTable(true, true, false);
        //    //difficulty_shape.ConvertToShape();
        //    difficulty_shape.Width = 375;
        //    difficulty_shape.Height = 220;
        //    difficulty_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    difficulty_rng.MoveStart(Word.WdUnits.wdParagraph, 1);
        //    difficulty_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
        //    difficulty_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    difficulty_rng.InsertParagraphAfter();
        //    ReleaseExcel(_wk, eapp);
        //    //oDoc.Characters.Last.InsertBreak(oParagrahbreak);
        }
        public void insertGroupDistChart(string title, DataTable dist_data)
        {
            DataTable dt = dist_data;
            double[][] data = new double[dt.Rows.Count][];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                data[i] = new double[2];
                data[i][0] = Convert.ToDouble((decimal)dt.Rows[i][0]);
                data[i][1] = Convert.ToDouble((int)dt.Rows[i][1]);

            }

            ZedGraph.createBar(data);
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
        //    Excel.Application eapp = new Excel.Application();
        //    eapp.Visible = false;
        //    Excel.Workbooks wk = eapp.Workbooks;
        //    Excel._Workbook _wk = wk.Add(oMissing);
        //    Excel.Sheets shs = _wk.Sheets;

        //    Word.InlineShape dist_shape;
        //    Word.Range dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;

            
            
        //    //Excel.Workbook dist_book = (Excel.Workbook)dist_shape.OLEFormat.Object;
        //    Excel.Worksheet dist_Sheet = shs.get_Item(1);

        //    dist_Sheet.Cells.Clear();

        //    //DataTable dist_data = ((WordData.group_data)_sdata.single_group_analysis[group_num]).group_dist;
        //    object[,] data = new object[dist_data.Rows.Count + 1, dist_data.Columns.Count + 1];
        //    int in_row = 0;
        //    foreach (DataRow dr1 in dist_data.Rows)
        //    {
        //        int col = 0;
        //        foreach (var item in dr1.ItemArray)
        //        {
        //            data[in_row, col] = item;
        //            col++;
        //        }
        //        in_row++;
        //    }

        //    dist_Sheet.get_Range("A1", "B" + dist_data.Rows.Count).Value2 = data;
        //    Excel.Chart chart_dist = _wk.Charts.Add(oMissing, dist_Sheet, oMissing, oMissing);

        //    Excel.Range dist_chart_rng = (Excel.Range)dist_Sheet.Cells[1, 1];

        //    chart_dist.ChartWizard(dist_chart_rng.CurrentRegion, Excel.XlChartType.xlColumnClustered, Type.Missing, Excel.XlRowCol.xlColumns, 1, 0, false, "", "分数", "人数", "");
        //    FormatExcel(_wk, "分数", "人数");

        //    dist_shape = dist_rng.InlineShapes.AddOLEObject(ref oClassType, _wk.Name,
        //ref oMissing, ref oMissing, ref oMissing,
        //ref oMissing, ref oMissing, ref oMissing);


        //    dist_shape.Range.InsertCaption(oWord.CaptionLabels["图"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
        //    dist_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
        //    dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;

        //    //dist_Sheet.UsedRange.CopyPicture();
        //    dist_shape.Width = 375;
        //    dist_shape.Height = 220;
        //    //dist_rng.PasteExcelTable(true, true, false);
        //    //dist_shape.ConvertToShape();
        //    dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    dist_rng.MoveStart(Word.WdUnits.wdParagraph, 1);
        //    dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
        //    dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    dist_rng.InsertParagraphAfter();
        //    ReleaseExcel(_wk, eapp);
        //    //oDoc.Characters.Last.InsertBreak(oParagrahbreak);
        }
        public void insertTotalDifficultyChart()
        {
            double[][] data = new double[_sdata.total_analysis.Rows.Count][];
            for (int i = 0; i < _sdata.total_analysis.Rows.Count; i++)
            {
                data[i] = new double[2];
                data[i][0] = Convert.ToDouble(_sdata.total_analysis.Rows[i]["difficulty"]);
                data[i][1] = Convert.ToDouble(_sdata.total_analysis.Rows[i]["discriminant"]);
            }

            ZedGraph.createGradient(data);
            Word.Range dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
            dist_rng.Paste();
            dist_rng.InsertCaption(oWord.CaptionLabels["图"], "    题目难度与区分度坐标图", oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            dist_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
            dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
            dist_rng.MoveStart(Word.WdUnits.wdParagraph, 1);
            dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            dist_rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            dist_rng.InsertParagraphAfter();
        //    Word.InlineShape diff_shape;
        //    Word.Range total_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;

        //    Excel.Application eapp = new Excel.Application();
        //    eapp.Visible = false;
        //    Excel.Workbooks wk = eapp.Workbooks;
        //    Excel._Workbook _wk = wk.Add(oMissing);
        //    Excel.Sheets shs = _wk.Sheets;
            
            
        //    //Excel.Workbook diff_book = (Excel.Workbook)diff_shape.OLEFormat.Object;
        //    Excel.Worksheet diff_Sheet = shs.get_Item(1);

        //    diff_Sheet.Cells.Clear();


        //    object[,] diff_data = new object[_sdata.total_analysis.Rows.Count + 1, 3];
        //    int row = 0;
        //    foreach (DataRow dr1 in _sdata.total_analysis.Rows)
        //    {
        //        diff_data[row, 0] = dr1["difficulty"];
        //        diff_data[row, 1] = dr1["discriminant"];


        //        row++;
        //    }

        //    diff_Sheet.get_Range("A1", "B" + _sdata.total_analysis.Rows.Count).Value2 = diff_data;
        //    Excel.Chart diff_chart_dist = _wk.Charts.Add(oMissing, diff_Sheet, oMissing, oMissing);

        //    Excel.Range diff_chart_rng = (Excel.Range)diff_Sheet.Cells[1, 1];

        //    diff_chart_dist.ChartWizard(diff_chart_rng.CurrentRegion, Excel.XlChartType.xlXYScatter, Type.Missing, Excel.XlRowCol.xlColumns, 1, 0, false, "", "分数", "人数", "");
        //    Excel.Series series;
        //    Excel.SeriesCollection collection = (Excel.SeriesCollection)diff_chart_dist.SeriesCollection(oMissing);
        //    series = collection.Item(1);
        //    series.MarkerStyle = Excel.XlMarkerStyle.xlMarkerStyleSquare;
        //    series.MarkerSize = 10;
        //    FormatExcel(_wk, "难度", "区分度");

        //    diff_shape = total_rng.InlineShapes.AddOLEObject(ref oClassType, _wk.Name,
        //ref oMissing, ref oMissing, ref oMissing,
        //ref oMissing, ref oMissing, ref oMissing);
        //    string diff_shape_title = "题目难度与区分度坐标图";
        //    diff_shape.Range.InsertCaption(oWord.CaptionLabels["图"], diff_shape_title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
        //    total_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
        //    total_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;

        //    //dist_Sheet.UsedRange.CopyPicture();
        //    diff_shape.Width = 375;
        //    diff_shape.Height = 220;
        //    //dist_rng.PasteExcelTable(true, true, false);
        //    //dist_shape.ConvertToShape();
        //    total_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    total_rng.MoveStart(Word.WdUnits.wdParagraph, 1);
        //    total_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
        //    total_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    total_rng.InsertParagraphAfter();
        //    ReleaseExcel(_wk, eapp);
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

        public void insertTotalTable(string title, DataRow dr)
        {
            Word.Table single_total_table;
            Word.Range single_table_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            single_total_table = oDoc.Tables.Add(single_table_range, 2, 9, ref oMissing, oTrue);
            single_total_table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);

            single_table_range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            single_table_range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            single_total_table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            single_total_table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;



            single_total_table.Cell(1, 1).Range.Text = "满分值";
            single_total_table.Cell(1, 2).Range.Text = "最大值";
            single_total_table.Cell(1, 3).Range.Text = "最小值";
            single_total_table.Cell(1, 4).Range.Text = "平均值";
            single_total_table.Cell(1, 5).Range.Text = "标准差";
            single_total_table.Cell(1, 6).Range.Text = "差异系数";
            single_total_table.Cell(1, 7).Range.Text = "难度";
            single_total_table.Cell(1, 8).Range.Text = "相关系数";
            single_total_table.Cell(1, 9).Range.Text = "鉴别指数";

            single_total_table.Cell(2, 1).Range.Text = Convert.ToInt32(dr["fullmark"]).ToString();
            single_total_table.Cell(2, 2).Range.Text = string.Format("{0:F1}", dr["max"]);
            single_total_table.Cell(2, 3).Range.Text = string.Format("{0:F1}", dr["min"]);
            single_total_table.Cell(2, 4).Range.Text = string.Format("{0:F2}", dr["avg"]);
            single_total_table.Cell(2, 5).Range.Text = string.Format("{0:F2}", dr["standardErr"]);
            single_total_table.Cell(2, 6).Range.Text = string.Format("{0:F2}", dr["dfactor"]);
            single_total_table.Cell(2, 7).Range.Text = string.Format("{0:F2}", dr["difficulty"]);
            single_total_table.Cell(2, 8).Range.Text = string.Format("{0:F2}", dr["correlation"]);
            single_total_table.Cell(2, 9).Range.Text = string.Format("{0:F2}", dr["discriminant"]);

            single_total_table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            single_table_range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            single_table_range.InsertParagraphAfter();
            //oDoc.Characters.Last.InsertBreak(oParagrahbreak);
        }

        public void insertChart(string title, DataTable dt, string x_axis, string y_axis, object type)
        {
            double[][] data = new double[dt.Rows.Count][];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                data[i] = new double[2];
                data[i][0] = Convert.ToDouble((decimal)dt.Rows[i][0]);
                data[i][1] = Convert.ToDouble((decimal)dt.Rows[i][1]);

            }

            ZedGraph.createDiffCuve(data, Convert.ToDouble(dt.Compute("Min([" + dt.Columns[0].ColumnName + "])", "")), Convert.ToDouble(dt.Compute("Max([" + dt.Columns[0].ColumnName + "])", "")));
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
           // Excel.Application eapp = new Excel.Application();
           // eapp.Visible = false;
           // Excel.Workbooks wk = eapp.Workbooks;
           // Excel._Workbook _wk = wk.Add(oMissing);
           // Excel.Sheets shs = _wk.Sheets;

           // Word.InlineShape dist_shape;
           // Word.Range dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
           

         
           // //Excel.Workbook dist_book = (Excel.Workbook)dist_shape.OLEFormat.Object;
           // Excel.Worksheet dist_Sheet = shs.get_Item(1);

           // dist_Sheet.Cells.Clear();

            
           // object[,] data = new object[dt.Rows.Count + 1, dt.Columns.Count + 1];
           // int in_row = 0;
           // foreach (DataRow dr1 in dt.Rows)
           // {
           //     int col = 0;
           //     foreach (var item in dr1.ItemArray)
           //     {
           //         data[in_row, col] = item;
           //         col++;
           //     }
           //     in_row++;
           // }
           // Excel.Range rng = dist_Sheet.Range[dist_Sheet.Cells[1][1], dist_Sheet.Cells[dt.Columns.Count][dt.Rows.Count]];
           // rng.Value2 = data;
           // Excel.Chart chart_dist = _wk.Charts.Add(oMissing, dist_Sheet, oMissing, oMissing);

           // Excel.Range dist_chart_rng = (Excel.Range)dist_Sheet.Cells[1, 1];

           // chart_dist.ChartWizard(dist_chart_rng.CurrentRegion, type, Type.Missing, Excel.XlRowCol.xlColumns, 1, 0, false, "", x_axis, y_axis, "");
           // FormatExcel(_wk, x_axis, y_axis);
           // dist_shape = dist_rng.InlineShapes.AddOLEObject(ref oClassType, _wk.Name,
           //ref oMissing, ref oMissing, ref oMissing,
           //ref oMissing, ref oMissing, ref oMissing);

           // dist_shape.Range.InsertCaption(oWord.CaptionLabels["图"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
           // dist_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
           // dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;

           // //dist_Sheet.UsedRange.CopyPicture();
           // dist_shape.Width = 375;
           // dist_shape.Height = 220;
           // //dist_rng.PasteExcelTable(true, true, false);
           // //dist_shape.ConvertToShape();
           // dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
           // dist_rng.MoveStart(Word.WdUnits.wdParagraph, 1);
           // dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
           // dist_rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
           // dist_rng.InsertParagraphAfter();
           // ReleaseExcel(_wk, eapp);
           // Thread.Sleep(2000);
            
        }
        public void insertMultipleChart(string title, DataTable dt, string x_axis, string y_axis, object type)
        {
            ZedGraph.createMultipleChoiceCuve(dt, x_axis, y_axis);
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


            //object[,] data = new object[dt.Rows.Count + 2, dt.Columns.Count + 1];
            //for (int i = 0; i < dt.Columns.Count; i++)
            //    data[0, i] = dt.Columns[i].ColumnName.ToString().Trim();
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
            //Excel.Range rng = dist_Sheet.Range[dist_Sheet.Cells[1][1], dist_Sheet.Cells[dt.Columns.Count][dt.Rows.Count+1]];
            //rng.Value2 = data;
            //Excel.Chart chart_dist = _wk.Charts.Add(oMissing, dist_Sheet, oMissing, oMissing);

            //Excel.Range dist_chart_rng = (Excel.Range)dist_Sheet.Cells[1, 1];

            //chart_dist.ChartWizard(dist_chart_rng.CurrentRegion, type, Type.Missing, Excel.XlRowCol.xlColumns, 1, 1, false, "", x_axis, y_axis, "");
            //if (dt.Columns.Count > 2)
            //{
            //    chart_dist.HasLegend = true;
            //    chart_dist.Legend.Position = Excel.XlLegendPosition.xlLegendPositionRight;
            //}
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
        public void insertGroupTable(string title, DataTable dt, WordData.single_type type)
        {

            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            
            table = oDoc.Tables.Add(range, dt.Rows.Count + 1, dt.Columns.Count, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            if (type == WordData.single_type.single || type == WordData.single_type.multiple)
                table.Cell(1, 1).Range.Text = "选项";
            else
                table.Cell(1, 1).Range.Text = "分值";
            for (int i = 1; i < dt.Columns.Count; i++)
            {
                if (dt.Columns[i].ColumnName.Trim().Equals("frequency"))
                    table.Cell(1, i + 1).Range.Text = "人数";
                else if (dt.Columns[i].ColumnName.Trim().Equals("rate"))
                    table.Cell(1, i + 1).Range.Text = "比率(%)";
                else if (dt.Columns[i].ColumnName.Trim().Equals("correlation"))
                {
                    if (type == WordData.single_type.sub)
                        table.Cell(1, i + 1).Range.Text = "平均值";
                    else
                        table.Cell(1, i + 1).Range.Text = "相关系数";
                }
                else
                    table.Cell(1, i + 1).Range.Text = dt.Columns[i].ColumnName;
            }
            int row = 2;
            foreach (DataRow dr in dt.Rows)
            {
                if (dr["mark"].ToString().Trim().Equals("未选") || dr["mark"].ToString().Trim().Equals("未选或多选"))
                {
                    for (int j = 0; j < dt.Columns.Count; j++)
                    {
                        if (dt.Columns[j].ColumnName.Trim().Equals("rate") ||
                                dt.Columns[j].ColumnName.Trim().Equals("correlation"))
                            table.Cell(dt.Rows.Count - 1, j + 1).Range.Text = string.Format("{0:F2}", dr[j]);
                        else if (dt.Columns[j].ColumnName.Trim().Equals("mark"))
                            table.Cell(dt.Rows.Count - 1, j + 1).Range.Text = dr[j].ToString().Trim();
                        else if (dt.Columns[j].ColumnName.Trim().Equals("frequency"))
                            table.Cell(dt.Rows.Count - 1, j + 1).Range.Text = dr[j].ToString();
                        else
                            table.Cell(dt.Rows.Count - 1, j + 1).Range.Text = Convert.ToInt32(dr[j]).ToString().Trim();
                    }
                }
                else if (dr["mark"].ToString().Trim().Equals("合计"))
                {
                    for (int j = 0; j < dt.Columns.Count; j++)
                    {
                        if (dt.Columns[j].ColumnName.Trim().Equals("rate"))

                            table.Cell(dt.Rows.Count, j + 1).Range.Text = string.Format("{0:F2}", dr[j]);
                        else if (dt.Columns[j].ColumnName.Trim().Equals("correlation"))
                            table.Cell(dt.Rows.Count, j + 1).Range.Text = "-";
                        else if (dt.Columns[j].ColumnName.Trim().Equals("mark"))
                            table.Cell(dt.Rows.Count, j + 1).Range.Text = dr[j].ToString().Trim();
                        else if (dt.Columns[j].ColumnName.Trim().Equals("frequency"))
                            table.Cell(dt.Rows.Count, j + 1).Range.Text = dr[j].ToString();
                        else
                            table.Cell(dt.Rows.Count, j + 1).Range.Text = Convert.ToInt32(dr[j]).ToString().Trim();
                    }
                }
                else if (dr["mark"].ToString().Trim().Equals("得分率"))
                {
                    for (int j = 0; j < dt.Columns.Count; j++)
                    {
                        if (dt.Columns[j].ColumnName.Trim().Equals("rate") ||
                            dt.Columns[j].ColumnName.Trim().Equals("correlation") ||
                            dt.Columns[j].ColumnName.Trim().Equals("frequency"))
                            table.Cell(dt.Rows.Count + 1, j + 1).Range.Text = "-";
                        else if (dt.Columns[j].ColumnName.Trim().Equals("mark"))
                            table.Cell(dt.Rows.Count + 1, j + 1).Range.Text = dr[j].ToString().Trim();
                        
                        else
                            table.Cell(dt.Rows.Count + 1, j + 1).Range.Text = string.Format("{0:F2}", dr[j]);
                    }
                }
                else
                {
                    for (int j = 0; j < dt.Columns.Count; j++)
                    {
                        if (dt.Columns[j].ColumnName.Trim().Equals("rate") ||
                            dt.Columns[j].ColumnName.Trim().Equals("correlation"))
                            table.Cell(row, j + 1).Range.Text = string.Format("{0:F2}", dr[j]);
                        else if (dt.Columns[j].ColumnName.Trim().Equals("mark"))
                            table.Cell(row, j + 1).Range.Text = dr[j].ToString().Trim();
                        else if (dt.Columns[j].ColumnName.Trim().Equals("frequency"))
                            table.Cell(row, j + 1).Range.Text = dr[j].ToString();
                        else
                            table.Cell(row, j + 1).Range.Text = Convert.ToInt32(dr[j]).ToString().Trim();
                    }
                    row++;
                }

            }

            table.Select();
            oWord.Selection.set_Style(ref TableContent2);
            changeStyle(table);
            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();
        }
        public void testcase()
        {
            object filepath = @"D:\项目\给王卅的编程资料\中考\template.dotx";
            object oMissing = System.Reflection.Missing.Value;
            Word.Application app = new Word.Application();
            app.DisplayAlerts = Word.WdAlertLevel.wdAlertsNone;

            object oEndOfDoc = "\\endofdoc";

            Word.Document doc = app.Documents.Add(ref filepath, ref oMissing, ref oMissing, ref oMissing);

            Word.Range range = doc.Paragraphs.Add(ref oMissing).Range;

            app.Visible = true;
            DataTable dt = new DataTable();
            dt.Columns.Add("type", typeof(string));
            dt.Columns.Add("c", typeof(decimal));
            dt.Columns.Add("c1", typeof(decimal));
            DataRow dr = dt.NewRow();
            dr["type"] = "G1";
            dr["c"] = 0.2m;
            dr["c1"] = 0.1m;
            dt.Rows.Add(dr);
            DataRow dr2 = dt.NewRow();
            dr2["type"] = "G2";
            dr2["c"] = 0.4m;
            dr2["c1"] = 0.3m;
            dt.Rows.Add(dr2);
            DataRow dr3 = dt.NewRow();
            dr3["type"] = "G3";
            dr3["c"] = 0.5m;
            dr3["c1"] = 0.2m;
            dt.Rows.Add(dr3);
            DataRow dr4 = dt.NewRow();
            dr4["type"] = "G4";
            dr4["c"] = 0.6m;
            dr4["c1"] = 0.7m;
            dt.Rows.Add(dr4);
            DataRow dr5 = dt.NewRow();
            dr5["type"] = "G5";
            dr5["c"] = 0.8m;
            dr5["c1"] = 0.7m;
            dt.Rows.Add(dr5);
            DataRow dr6 = dt.NewRow();
            dr6["type"] = "G6";
            dr6["c"] = 0.9m;
            dr6["c1"] = 0.8m;
            dt.Rows.Add(dr6);
            ZedGraph.createMultipleChoiceCuve(dt, "分数", "难度(%)人民");
            Word.Range dist_rng = doc.Bookmarks.get_Item(oEndOfDoc).Range;
            dist_rng.Paste();
            doc.Characters.Last.InsertBreak(Microsoft.Office.Interop.Word.WdBreakType.wdPageBreak);
            dist_rng = doc.Paragraphs.Add(ref oMissing).Range;
            dist_rng.set_Style(ExamTitle1);
            dist_rng.InsertBefore("next page" + "\n");
        }
        public void createCuve(string cuveBmpPath, double[][] cuveData, double maxSource, Word.Range dist_rng)
        {

            ZedGraphControl zgc = new ZedGraphControl();
            GraphPane myPane = zgc.GraphPane;
            zgc.Width = 531;
            zgc.Height = 350;


            // Set the title and axis labels
            myPane.Title.Text = " ";
            myPane.XAxis.Title.Text = "分数";
            myPane.YAxis.Title.Text = "难度";

            // Enter some calculated data constants
            double[] x = new double[cuveData.Length];
            double[] y = new double[cuveData.Length];

            for (int i = 0; i < cuveData.Length; i++)
            {
                x[i] = cuveData[i][0];
                y[i] = cuveData[i][1];

            }


            double[] cuveX = new double[cuveData.Length];
            double[] cuveY = new double[cuveData.Length];

            for (int i = 0; i < cuveData.Length; i++)
            {
                cuveX[i] = cuveData[i][0];
                cuveY[i] = cuveData[i][1];

            }
            PointPairList ppCurve = new PointPairList(cuveX, cuveY);
            LineItem myCurve = myPane.AddCurve("", ppCurve, Color.Red, SymbolType.Diamond);
            myCurve.Line.Style = System.Drawing.Drawing2D.DashStyle.Dot;
            myCurve.Line.Width = 5;
            myCurve.Line.IsSmooth = true;
            //myCurve.Symbol.Size = 12;
            
            //myCurve.Line.SmoothTension = 1F;//
            //myCurve.Symbol.Type = SymbolType.None;



            // Turn off the line, so the curve will by symbols only
            myCurve.Line.IsVisible = true;



            // Set the x and y scale and title font sizes to 14
            myPane.XAxis.Scale.FontSpec.Size = 14;
            myPane.XAxis.Title.FontSpec.Size = 14;
            myPane.YAxis.Scale.FontSpec.Size = 14;
            myPane.YAxis.Title.FontSpec.Size = 14;
            myPane.XAxis.Title.FontSpec.Family = "黑体";
            myPane.YAxis.Title.FontSpec.Family = "黑体";



            myPane.XAxis.Scale.Max = maxSource;
            myPane.XAxis.Scale.Min = 0.00;
            myPane.XAxis.Scale.MajorStep = maxSource / 10;
            myPane.YAxis.Scale.Max = 50.00;
            //myPane.YAxis.Scale.MajorStep = 0.5;

            // Set the GraphPane title font size to 16
            myPane.Title.FontSpec.Size = 16;
            // Turn off the legend
            myPane.Legend.IsVisible = false;
            myPane.Title.IsVisible = true;
            myPane.Chart.Fill = new Fill(Color.White);
            zgc.AxisChange();


            //添加一条线
            LineObj line = new LineObj(0.00, 0.5, maxSource, 0.50);
            line.Line.Style = System.Drawing.Drawing2D.DashStyle.DashDot;
            
            line.Line.DashOn = 10f;
            line.Line.DashOff = 8f;
            line.IsClippedToChartRect = true;
            line.Line.Color = Color.LightSteelBlue;
            line.ZOrder = ZOrder.F_BehindGrid;
            line.Location.AlignH = AlignH.Left;
            line.Location.AlignV = AlignV.Top;
            line.Location.CoordinateFrame = CoordType.AxisXYScale;
            myPane.GraphObjList.Add(line);


            zgc.AxisChange();
            BarItem.CreateBarLabels(myPane, false, null);
            zgc.Refresh();

            Bitmap sourceBitmap = new Bitmap(zgc.Width, zgc.Height);
            
            zgc.DrawToBitmap(sourceBitmap, new Rectangle(0, 0, zgc.Width, zgc.Height));
            Clipboard.SetImage(sourceBitmap);
            dist_rng.Paste();
            //sourceBitmap.Save(cuveBmpPath + @"\testCuve.bmp");
            //dist_rng.InlineShapes.AddPicture(sourceBitmap., false, true, dist_rng);
        }
        public void insertTotalChart(string title, WordData sdata)
        {
            DataTable dt = sdata.totalmark_dist;
            double[][] data = new double[dt.Rows.Count][];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                data[i] = new double[2];
                data[i][0] = Convert.ToDouble((decimal)dt.Rows[i][0]);
                data[i][1] = Convert.ToDouble((int)dt.Rows[i][1]);

            }
            double[] cuvedata = new double[2];
            cuvedata[0] = Convert.ToDouble(sdata.avg);
            cuvedata[1] = Convert.ToDouble(sdata.stDev);
            ZedGraph.createCuveAndBar(cuvedata, data, Convert.ToDouble(sdata.max));
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
        //    Excel.Application eapp = new Excel.Application();
        //    eapp.Visible = false;
        //    Excel.Workbooks wk = eapp.Workbooks;
        //    Excel._Workbook _wk = wk.Add(oMissing);
        //    Excel.Sheets shs = _wk.Sheets;

        //    Word.InlineShape total_dist_shape;
        //    Word.Range total_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;


        //    Excel.Worksheet total_dist_Sheet = shs.get_Item(1);

        //    total_dist_Sheet.Cells.Clear();

        //    DataTable total_dist_data = sdata.totalmark_dist;
        //    object[,] total_data = new object[total_dist_data.Rows.Count + 1, total_dist_data.Columns.Count + 1];
        //    int row = 0;
        //    foreach (DataRow dr1 in total_dist_data.Rows)
        //    {
        //        int col = 0;
        //        foreach (var item in dr1.ItemArray)
        //        {
        //            total_data[row, col] = item;
        //            col++;
        //        }
        //        row++;
        //    }

        //    total_dist_Sheet.get_Range("A1", "B" + total_dist_data.Rows.Count).Value2 = total_data;
        //    Excel.Chart total_chart_dist = _wk.Charts.Add(oMissing, total_dist_Sheet, oMissing, oMissing);

        //    Excel.Range total_dist_chart_rng = (Excel.Range)total_dist_Sheet.Cells[1, 1];

        //    total_chart_dist.ChartWizard(total_dist_chart_rng.CurrentRegion, Excel.XlChartType.xlColumnClustered, Type.Missing, Excel.XlRowCol.xlColumns, 1, 0, false, "", "分数", "人数", "");

        //    FormatExcel(_wk, "分数", "人数");
        //    total_dist_shape = total_rng.InlineShapes.AddOLEObject(ref oClassType, _wk.Name,
        //ref oMissing, ref oMissing, ref oMissing,
        //ref oMissing, ref oMissing, ref oMissing);
        //    total_dist_shape.Range.InsertCaption(oWord.CaptionLabels["图"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
        //    total_rng.MoveEnd(Word.WdUnits.wdParagraph, 1);
        //    total_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
        //    //total_dist_shape.Range.
        //    //dist_Sheet.UsedRange.CopyPicture();
        //    total_dist_shape.Width = 375;
        //    total_dist_shape.Height = 220;
        //    //total_dist_shape.Range.ShapeRange.Align(Microsoft.Office.Core.MsoAlignCmd.msoAlignCenters, );
        //    //dist_rng.PasteExcelTable(true, true, false);
        //    //dist_shape.ConvertToShape();

        //    total_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    total_rng.MoveStart(Word.WdUnits.wdParagraph, 1);
        //    total_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
        //    total_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
        //    total_rng.InsertParagraphAfter();
            
            
        //    ReleaseExcel(_wk, eapp);
            //oDoc.Characters.Last.InsertBreak(oParagrahbreak);
        }
        public void FormatExcel(Excel._Workbook _wk, string xstring, string ystring)
        {
            Excel.Axis yaxis = _wk.ActiveChart.Axes(Excel.XlAxisType.xlValue, Excel.XlAxisGroup.xlPrimary);
            if (_wk.ActiveChart.HasLegend)
            {
                _wk.ActiveChart.Legend.Format.TextFrame2.TextRange.Font.Size = 20;
                _wk.ActiveChart.Legend.Format.TextFrame2.TextRange.Font.Name = "Times New Roman";
            }
            yaxis.HasTitle = true;
            yaxis.AxisTitle.Characters.Text = ystring;
            yaxis.AxisTitle.Characters.Font.Size = 20;
            yaxis.TickLabels.Font.Name = "Times New Roman";
            yaxis.TickLabels.Font.Size = 20;
            yaxis.MinimumScale = 0;
            yaxis.MaximumScaleIsAuto = true;

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
            for(int i = 0; i < th_string.Length; i++)
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
                if(i < th_string.Length - 1)
                    first.InsertAfter("、");
            }
            first.InsertParagraphAfter();

            //oDoc.Characters.Last.Select();
            //oWord.Selection.HomeKey(Word.WdUnits.wdLine, oMissing);
            //oWord.Selection.Delete(Word.WdUnits.wdCharacter, oMissing);
            //oWord.Selection.Range.set_Style(ExamBodyText);
        }

        void changeStyle(Word.Table table)
        {
            for (int i = 1; i <= table.Rows.Count; i++)
            {
                if (table.Cell(i, 1).Range.Text.Equals("未选或多选\r\a"))
                {
                    table.Cell(i, 1).Range.Select();
                    oWord.Selection.Range.Font.Size = 9f;
                }
            }
        }
    }
}
