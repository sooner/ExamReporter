using System;
using System.Collections.Generic;
using System.Data;
using Word = Microsoft.Office.Interop.Word;
using Excel = Microsoft.Office.Interop.Excel;
using System.Linq;
using System.Text;

using System.Runtime.InteropServices;

namespace ExamReport
{
    class ZF_wordcreator
    {
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
        object oParagrahbreak = Microsoft.Office.Interop.Word.WdBreakType.wdLineBreak;
        Object oTrue = true;
        Object oFalse = false;

        object oClassType = "Excel.Chart.8";
        public void total_create(ZF_statistic data)
        {
            object filepath = @Utils.CurrentDirectory + @"\template.dotx";
            //object filepath = @"D:\项目\给王卅的编程资料\中考\c.dotx";
            //Start Word and create a new document.

            oWord = new Word.Application();

            oWord.Visible = Utils.isVisible;
            oDoc = oWord.Documents.Add(ref filepath, ref oMissing,
            ref oMissing, ref oMissing);
            Utils.WriteFrontPage(oDoc);

            insertText(ExamTitle1, "试卷整体分析");
            insertTotalTable_final("    试卷总分分析表", data);
            insertText(ExamTitle1, "总分分布曲线图");
            Partition_wordcreator.ChartCombine comb = new Partition_wordcreator.ChartCombine();
            comb.Add(data.w_result.dist, "文科");
            comb.Add(data.l_result.dist, "理科");
            insertChart("    总分分布曲线图", comb.target, "分数", "人数", Excel.XlChartType.xlLineMarkers);
            insertText(ExamTitle1, "各科目难度曲线图");
            insertText(ExamTitle3, "语文学科");
            insertChart("    语文学科难度曲线图", data.sub[0], "分数", "难度", Excel.XlChartType.xlLineMarkers);
            insertText(ExamTitle3, "数学文科");
            insertChart("    数学文科难度曲线图", data.sub[1], "分数", "难度", Excel.XlChartType.xlLineMarkers);
            insertText(ExamTitle3, "数学理科");
            insertChart("    数学理科难度曲线图", data.sub[2], "分数", "难度", Excel.XlChartType.xlLineMarkers);
            insertText(ExamTitle3, "英语学科");
            insertChart("    英语学科难度曲线图", data.sub[3], "分数", "难度", Excel.XlChartType.xlLineMarkers);
            insertText(ExamTitle3, "文科综合");
            insertChart("    文科综合难度曲线图", data.sub[4], "分数", "难度", Excel.XlChartType.xlLineMarkers);
            insertText(ExamTitle3, "理科综合");
            insertChart("    理科综合难度曲线图", data.sub[5], "分数", "难度", Excel.XlChartType.xlLineMarkers);

            insertText(ExamTitle1, "总分分数分布图");
            insertFreqTable_final("    总分分数分布表", data);

            foreach (Word.TableOfContents table in oDoc.TablesOfContents)
                table.Update();
            Utils.Save(oDoc, oWord);
        }
        void insertFreqTable_final(string title, ZF_statistic temp)
        {
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            table = oDoc.Tables.Add(range, 1, 6, ref oMissing, oTrue);

            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;


            table.Cell(1, 1).Range.Text = "分值";
            table.Cell(1, 2).Range.Text = "分类";
            table.Cell(1, 3).Range.Text = "人数";
            table.Cell(1, 4).Range.Text = "比率(%)";
            table.Cell(1, 5).Range.Text = "累计人数";
            table.Cell(1, 6).Range.Text = "累计比率(%)";


            List<DataTable> fz_data = new List<DataTable>();

            fz_data.Add(temp.w_result.frequency);

            fz_data.Add(temp.l_result.frequency);
            

            string[] name = { "文科", "理科" };
            string[] lastFreq = { "0", "0" };
            string[] lastRate = { "0.00", "0.00" };

            insertTableData(table, fz_data, name);
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            verticalCellMerge(table, 2, 1);


            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();
        }
        
        void insertTotalTable_final(string title, ZF_statistic data)
        {
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            table = oDoc.Tables.Add(range, 3, 9, ref oMissing, oTrue);
            table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
            //range.MoveEnd(Word.WdUnits.wdParagraph, 1);
            range.Select();
            range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            
            
            table.Rows[1].HeadingFormat = -1;


            table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
            table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;

            //table.Cell(1, 1).Range.Text = "分类";
            table.Cell(1, 2).Range.Text = "人数";
            table.Cell(1, 3).Range.Text = "满分值";
            table.Cell(1, 4).Range.Text = "最大值";
            table.Cell(1, 5).Range.Text = "最小值";
            table.Cell(1, 6).Range.Text = "平均值";
            table.Cell(1, 7).Range.Text = "标准差";
            table.Cell(1, 8).Range.Text = "差异系数";
            table.Cell(1, 9).Range.Text = "得分率";

            table.Cell(2, 1).Range.Text = "文科";
            table.Cell(2, 2).Range.Text = data.w_result.total_num.ToString();
            table.Cell(2, 3).Range.Text = string.Format("{0:F1}", data.w_result.fullmark);
            table.Cell(2, 4).Range.Text = string.Format("{0:F1}", data.w_result.max);
            table.Cell(2, 5).Range.Text = string.Format("{0:F1}", data.w_result.min);
            table.Cell(2, 6).Range.Text = string.Format("{0:F1}", data.w_result.avg);
            table.Cell(2, 7).Range.Text = string.Format("{0:F2}", data.w_result.stDev);
            table.Cell(2, 8).Range.Text = string.Format("{0:F2}", data.w_result.Dfactor);
            table.Cell(2, 9).Range.Text = string.Format("{0:F2}", data.w_result.difficulty);

            table.Cell(3, 1).Range.Text = "理科";
            table.Cell(3, 2).Range.Text = data.l_result.total_num.ToString();
            table.Cell(3, 3).Range.Text = string.Format("{0:F1}", data.l_result.fullmark);
            table.Cell(3, 4).Range.Text = string.Format("{0:F1}", data.l_result.max);
            table.Cell(3, 5).Range.Text = string.Format("{0:F1}", data.l_result.min);
            table.Cell(3, 6).Range.Text = string.Format("{0:F1}", data.l_result.avg);
            table.Cell(3, 7).Range.Text = string.Format("{0:F2}", data.l_result.stDev);
            table.Cell(3, 8).Range.Text = string.Format("{0:F2}", data.l_result.Dfactor);
            table.Cell(3, 9).Range.Text = string.Format("{0:F2}", data.l_result.difficulty);

            table.Select();
            oWord.Selection.set_Style(ref TableContent2);

            range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            range.InsertParagraphAfter();
        }
        public void partition_wordcreate(List<ZF_statistic> data, string _subject)
        {
            object filepath = @Utils.CurrentDirectory + @"\template.dotx";
            //object filepath = @"D:\项目\给王卅的编程资料\中考\c.dotx";
            //Start Word and create a new document.

            oWord = new Word.Application();

            oWord.Visible = Utils.isVisible;
            oDoc = oWord.Documents.Add(ref filepath, ref oMissing,
            ref oMissing, ref oMissing);
            Utils.WriteFrontPage(oDoc);

            List<ZF_statistic> temp;
            if (data.Count > 2)
            {
                temp = new List<ZF_statistic>();
                for (int i = 5; i < data.Count; i++)
                    temp.Add(data[i]);
            }
            else
                temp = data;


            object oPageBreak = Microsoft.Office.Interop.Word.WdBreakType.wdPageBreak;
            insertText(ExamTitle1, "文科试卷总分分析");
            insertTotalTable("文科试卷总分分析表", data, true);
            insertText(ExamTitle1, "文科总分分布曲线图");
            Partition_wordcreator.ChartCombine comb = new Partition_wordcreator.ChartCombine();
            if (data.Count > 2)
            {
                comb.Add(data[5].w_result.dist, data[5]._name);
            }
            else
                foreach (ZF_statistic stat in data)
                    comb.Add(stat.w_result.dist, stat._name);
            insertChart("总分分布曲线图", comb.target, "分数", "人数百分比", Excel.XlChartType.xlLineMarkers);
            

            insertFreqTable("文科总分分数分布表", temp, true);


            insertText(ExamTitle1, "理科试卷总分分析");
            insertTotalTable("理科试卷总分分析表", data, false);
            insertText(ExamTitle1, "理科总分分布曲线图");
            comb = new Partition_wordcreator.ChartCombine();
            if (data.Count > 2)
            {
                comb.Add(data[5].l_result.dist, data[5]._name);
            }
            else
                foreach (ZF_statistic stat in data)
                    comb.Add(stat.l_result.dist, stat._name);
            insertChart("总分分布曲线图", comb.target, "分数", "人数百分比", Excel.XlChartType.xlLineMarkers);
            insertFreqTable("理科总分分数分布表", temp, false);

            foreach (Word.TableOfContents table in oDoc.TablesOfContents)
                table.Update();
            Utils.Save(oDoc, oWord);
        }
        public void insertFreqTable(string title, List<ZF_statistic> data, bool isWenke)
        {
           
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            if (data.Count > 2)
            {
                int count;
                if (isWenke)
                    count = data[0].w_result.frequency.Rows.Count;
                else
                    count = data[0].l_result.frequency.Rows.Count;

                table = oDoc.Tables.Add(range, count * data.Count + 1, 6, ref oMissing, oTrue);

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



                string[] lastFreq = new string[data.Count];
                string[] lastRate = new string[data.Count];
                for (int i = 0; i < data.Count; i++)
                {
                    lastFreq[i] = "0";
                    lastRate[i] = "0.00";
                }
                for (int i = 0; i < count; i++)
                {
                    int tablerow = i * data.Count + 2;
                    decimal primarykey;
                    DataTable freq;
                    if (isWenke)
                        primarykey = (decimal)data[0].w_result.frequency.Rows[i]["totalmark"];
                    else
                        primarykey = (decimal)data[0].l_result.frequency.Rows[i]["totalmark"];
                    int j = 0;
                    foreach (ZF_statistic stat in data)
                    {
                        if (isWenke)
                            freq = stat.w_result.frequency;
                        else
                            freq = stat.l_result.frequency;
                        if (freq.Rows.Contains(primarykey))
                        {
                            table.Cell(tablerow, 1).Range.Text = string.Format("{0:F1}", freq.Rows.Find(primarykey)["totalmark"]);
                            table.Cell(tablerow, 2).Range.Text = stat._name;
                            table.Cell(tablerow, 3).Range.Text = freq.Rows.Find(primarykey)["frequency"].ToString().Trim();
                            table.Cell(tablerow, 4).Range.Text = string.Format("{0:F2}", freq.Rows.Find(primarykey)["rate"]);
                            table.Cell(tablerow, 5).Range.Text = freq.Rows.Find(primarykey)["accumulateFreq"].ToString().Trim();
                            table.Cell(tablerow, 6).Range.Text = string.Format("{0:F2}", freq.Rows.Find(primarykey)["accumulateRate"]);

                            lastFreq[j] = freq.Rows.Find(primarykey)["accumulateFreq"].ToString().Trim();
                            lastRate[j] = string.Format("{0:F2}", freq.Rows.Find(primarykey)["accumulateRate"]);
                        }
                        else
                        {
                            table.Cell(tablerow, 1).Range.Text = string.Format("{0:F1}", primarykey);
                            table.Cell(tablerow, 2).Range.Text = stat._name;
                            table.Cell(tablerow, 3).Range.Text = "0";
                            table.Cell(tablerow, 4).Range.Text = "0.00";
                            table.Cell(tablerow, 5).Range.Text = lastFreq[j];
                            table.Cell(tablerow, 6).Range.Text = lastRate[j];
                        }
                        tablerow++;
                        j++;
                    }
                }
            }
            else
            {
                table = oDoc.Tables.Add(range, 1, 6, ref oMissing, oTrue);

                table.Range.InsertCaption(oWord.CaptionLabels["表"], title, oMissing, Word.WdCaptionPosition.wdCaptionPositionAbove, oMissing);
                range.MoveEnd(Word.WdUnits.wdParagraph, 1);
                range.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
                

                table.Cell(1, 1).Range.Text = "分值";
                table.Cell(1, 2).Range.Text = "分类";
                table.Cell(1, 3).Range.Text = "人数";
                table.Cell(1, 4).Range.Text = "比率(%)";
                table.Cell(1, 5).Range.Text = "累计人数";
                table.Cell(1, 6).Range.Text = "累计比率(%)";


                List<DataTable> fz_data = new List<DataTable>();
                foreach (ZF_statistic temp in data)
                {
                    if (isWenke)
                        fz_data.Add(temp.w_result.frequency);
                    else
                        fz_data.Add(temp.l_result.frequency);
                }

                string[] name = {data[0]._name, data[1]._name };
                string[] lastFreq = { "0", "0" };
                string[] lastRate = { "0.00", "0.00"};

                insertTableData(table, fz_data, name);
                table.Rows[1].HeadingFormat = -1;


                table.Borders.OutsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
                table.Borders.InsideLineStyle = Word.WdLineStyle.wdLineStyleSingle;
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
                    table.Cell(previousRowIndex, columnIndex).Range.Text = currentText.Trim('\a').Trim('\r');  // 因为合并后并没有将单元格内容去除，需要手动修改
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

        public void insertTableData(Word.Table table, List<DataTable> data, string[] name)
        {
            int i = 0;
            int j = 0;
            string[] lastFreq = { "0", "0" };
            string[] lastRate = { "0.00", "0.00" };
            while (true)
            {
                if (i < data[0].Rows.Count && j < data[1].Rows.Count)
                {
                    if ((decimal)data[0].Rows[i]["totalmark"] > (decimal)data[1].Rows[j]["totalmark"])
                    {
                        Word.Row row = table.Rows.Add();
                        row.Cells[1].Range.Text = string.Format("{0:F1}", data[0].Rows[i]["totalmark"]);
                        row.Cells[2].Range.Text = name[0];
                        row.Cells[3].Range.Text = data[0].Rows[i]["frequency"].ToString();
                        row.Cells[4].Range.Text = string.Format("{0:F2}", data[0].Rows[i]["rate"]);
                        row.Cells[5].Range.Text = data[0].Rows[i]["accumulateFreq"].ToString();
                        row.Cells[6].Range.Text = string.Format("{0:F2}", data[0].Rows[i]["accumulateRate"]);
                        Word.Row row2 = table.Rows.Add();
                        row2.Cells[1].Range.Text = string.Format("{0:F1}", data[0].Rows[i]["totalmark"]);
                        row2.Cells[2].Range.Text = name[1];
                        row2.Cells[3].Range.Text = "0";
                        row2.Cells[4].Range.Text = "0.00";
                        row2.Cells[5].Range.Text = lastFreq[1];
                        row2.Cells[6].Range.Text = lastRate[1];
                        lastFreq[0] = data[0].Rows[i]["accumulateFreq"].ToString();
                        lastRate[0] = string.Format("{0:F2}", data[0].Rows[i]["accumulateRate"]);
                        i++;

                    }
                    else if ((decimal)data[0].Rows[i]["totalmark"] < (decimal)data[1].Rows[j]["totalmark"])
                    {
                        Word.Row row = table.Rows.Add();
                        row.Cells[1].Range.Text = string.Format("{0:F1}", data[1].Rows[j]["totalmark"]);
                        row.Cells[2].Range.Text = name[0];
                        row.Cells[3].Range.Text = "0";
                        row.Cells[4].Range.Text = "0.00";
                        row.Cells[5].Range.Text = lastFreq[0];
                        row.Cells[6].Range.Text = lastRate[0];
                        Word.Row row2 = table.Rows.Add();
                        row2.Cells[1].Range.Text = string.Format("{0:F1}", data[1].Rows[j]["totalmark"]);
                        row2.Cells[2].Range.Text = name[1];
                        row2.Cells[3].Range.Text = data[1].Rows[j]["frequency"].ToString();
                        row2.Cells[4].Range.Text = string.Format("{0:F2}", data[1].Rows[j]["rate"]);
                        row2.Cells[5].Range.Text = data[1].Rows[j]["accumulateFreq"].ToString();
                        row2.Cells[6].Range.Text = string.Format("{0:F2}", data[1].Rows[j]["accumulateRate"]);
                        lastFreq[1] = data[1].Rows[j]["accumulateFreq"].ToString();
                        lastRate[1] = string.Format("{0:F2}", data[1].Rows[j]["accumulateRate"]);
                        j++;

                    }
                    else
                    {
                        Word.Row row = table.Rows.Add();
                        row.Cells[1].Range.Text = string.Format("{0:F1}", data[0].Rows[i]["totalmark"]);
                        row.Cells[2].Range.Text = name[0];
                        row.Cells[3].Range.Text = data[0].Rows[i]["frequency"].ToString();
                        row.Cells[4].Range.Text = string.Format("{0:F2}", data[0].Rows[i]["rate"]);
                        row.Cells[5].Range.Text = data[0].Rows[i]["accumulateFreq"].ToString();
                        row.Cells[6].Range.Text = string.Format("{0:F2}", data[0].Rows[i]["accumulateRate"]);
                        Word.Row row2 = table.Rows.Add();
                        row2.Cells[1].Range.Text = string.Format("{0:F1}", data[1].Rows[j]["totalmark"]);
                        row2.Cells[2].Range.Text = name[1];
                        row2.Cells[3].Range.Text = data[1].Rows[j]["frequency"].ToString();
                        row2.Cells[4].Range.Text = string.Format("{0:F2}", data[1].Rows[j]["rate"]);
                        row2.Cells[5].Range.Text = data[1].Rows[j]["accumulateFreq"].ToString();
                        row2.Cells[6].Range.Text = string.Format("{0:F2}", data[1].Rows[j]["accumulateRate"]);

                        lastFreq[0] = data[0].Rows[i]["accumulateFreq"].ToString();
                        lastRate[0] = string.Format("{0:F2}", data[0].Rows[i]["accumulateRate"]);
                        lastFreq[1] = data[1].Rows[j]["accumulateFreq"].ToString();
                        lastRate[1] = string.Format("{0:F2}", data[1].Rows[j]["accumulateRate"]);
                        i++;
                        j++;
                    }
                }
                else if (i >= data[0].Rows.Count && j < data[1].Rows.Count)
                {
                    Word.Row row = table.Rows.Add();
                    row.Cells[1].Range.Text = string.Format("{0:F1}", data[1].Rows[j]["totalmark"]);
                    row.Cells[2].Range.Text = name[0];
                    row.Cells[3].Range.Text = "0";
                    row.Cells[4].Range.Text = "0.00";
                    row.Cells[5].Range.Text = lastFreq[0];
                    row.Cells[6].Range.Text = lastRate[0];
                    Word.Row row2 = table.Rows.Add();
                    row2.Cells[1].Range.Text = string.Format("{0:F1}", data[1].Rows[j]["totalmark"]);
                    row2.Cells[2].Range.Text = name[1];
                    row2.Cells[3].Range.Text = data[1].Rows[j]["frequency"].ToString();
                    row2.Cells[4].Range.Text = string.Format("{0:F2}", data[1].Rows[j]["rate"]);
                    row2.Cells[5].Range.Text = data[1].Rows[j]["accumulateFreq"].ToString();
                    row2.Cells[6].Range.Text = string.Format("{0:F2}", data[1].Rows[j]["accumulateRate"]);
                    lastFreq[1] = data[1].Rows[j]["accumulateFreq"].ToString();
                    lastRate[1] = string.Format("{0:F2}", data[1].Rows[j]["accumulateRate"]);
                    j++;
                }
                else if (i < data[0].Rows.Count && j >= data[1].Rows.Count)
                {
                    Word.Row row = table.Rows.Add();
                    row.Cells[1].Range.Text = string.Format("{0:F1}", data[0].Rows[i]["totalmark"]);
                    row.Cells[2].Range.Text = name[0];
                    row.Cells[3].Range.Text = data[0].Rows[i]["frequency"].ToString();
                    row.Cells[4].Range.Text = string.Format("{0:F2}", data[0].Rows[i]["rate"]);
                    row.Cells[5].Range.Text = data[0].Rows[i]["accumulateFreq"].ToString();
                    row.Cells[6].Range.Text = string.Format("{0:F2}", data[0].Rows[i]["accumulateRate"]);
                    Word.Row row2 = table.Rows.Add();
                    row2.Cells[1].Range.Text = string.Format("{0:F1}", data[0].Rows[i]["totalmark"]);
                    row2.Cells[2].Range.Text = name[1];
                    row2.Cells[3].Range.Text = "0";
                    row2.Cells[4].Range.Text = "0.00";
                    row2.Cells[5].Range.Text = lastFreq[1];
                    row2.Cells[6].Range.Text = lastRate[1];
                    lastFreq[0] = data[0].Rows[i]["accumulateFreq"].ToString();
                    lastRate[0] = string.Format("{0:F2}", data[0].Rows[i]["accumulateRate"]);
                    i++;
                }
                else
                    break;
                
                
            }
        }
        public void insertChart(string title, DataTable dt, string x_axis, string y_axis, object type)
        {
            if (dt.Columns.Count > 2)
            {
                ZedGraph.createMultipleCuve(dt, "分数", "人数", Convert.ToDouble(dt.Compute("Min([" + dt.Columns[0].ColumnName + "])", "")), Convert.ToDouble(dt.Compute("Max([" + dt.Columns[0].ColumnName + "])", "")));
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
            //rng.Value2 = data;
            //Excel.Chart chart_dist = _wk.Charts.Add(oMissing, dist_Sheet, oMissing, oMissing);

            //Excel.Range dist_chart_rng = (Excel.Range)dist_Sheet.Cells[1, 1];

            //chart_dist.ChartWizard(dist_chart_rng.CurrentRegion, type, Type.Missing, Excel.XlRowCol.xlColumns, 1, 1, false, "", x_axis, y_axis, "");
            //if (dt.Columns.Count > 2)
            //    _wk.ActiveChart.HasLegend = true;
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
            dist_rng = oDoc.Bookmarks.get_Item(oEndOfDoc).Range;
            dist_rng.MoveStart(Word.WdUnits.wdParagraph, 1);
            dist_rng.Paragraphs.Alignment = Word.WdParagraphAlignment.wdAlignParagraphCenter;
            dist_rng = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;
            dist_rng.InsertParagraphAfter();
            //ReleaseExcel(_wk, eapp);
        }
        public void insertTotalTable(string title, List<ZF_statistic> totaldata, bool isWenke)
        {
            Word.Table table;
            Word.Range range = oDoc.Bookmarks.get_Item(ref oEndOfDoc).Range;

            table = oDoc.Tables.Add(range, totaldata.Count + 1, 9, ref oMissing, oTrue);
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

            for (int i = 0; i < totaldata.Count; i++)
            {
                ZF_worddata temp;
                if (isWenke)
                    temp = totaldata[i].w_result;
                else
                    temp = totaldata[i].l_result;
                table.Cell(i + 2, 1).Range.Text = totaldata[i]._name;
                table.Cell(i + 2, 2).Range.Text = temp.total_num.ToString();
                table.Cell(i + 2, 3).Range.Text = string.Format("{0:F2}", temp.fullmark);
                table.Cell(i + 2, 4).Range.Text = string.Format("{0:F2}", temp.max);
                table.Cell(i + 2, 5).Range.Text = string.Format("{0:F2}", temp.min);
                table.Cell(i + 2, 6).Range.Text = string.Format("{0:F2}", temp.avg);
                table.Cell(i + 2, 7).Range.Text = string.Format("{0:F2}", temp.stDev);
                table.Cell(i + 2, 8).Range.Text = string.Format("{0:F2}", temp.Dfactor);
                table.Cell(i + 2, 9).Range.Text = string.Format("{0:F2}", temp.difficulty);
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
    }
}
