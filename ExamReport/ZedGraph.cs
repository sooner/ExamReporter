using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Drawing;
using System.Windows.Forms;
using ZedGraph;
using Microsoft.Practices.EnterpriseLibrary.Common;
using System.Threading;
using myStyle = System.Drawing.Drawing2D.DashStyle;

namespace ExamReport
{
    public static class ZedGraph
    {
        public static List<Color> mycolor = new List<Color>{ Color.Red, Color.Blue, Color.Black, Color.Gray, Color.Brown, Color.Chocolate, Color.Purple, Color.Gray};
        public static List<SymbolType> mySymbol = new List<SymbolType> { SymbolType.Circle, SymbolType.Square, SymbolType.Diamond, SymbolType.Star };
        public static List<myStyle> myDashStyle = new List<System.Drawing.Drawing2D.DashStyle> { myStyle.Custom, myStyle.Dash, myStyle.DashDot, myStyle.DashDotDot, myStyle.Solid, myStyle.Dot, myStyle.Custom, myStyle.Dash, myStyle.DashDot };
        public static List<float> myWidth = new List<float> { 2, 5, 2, 5, 2, 5, 5, 2, 5, 2, 5, 2};
        public static void createDiffCuve(double[][] cuveData, double minX, double maxX)
        {
            createCuve("分数", "难度", cuveData, minX, Convert.ToDouble(Utils.fullmark), 1.0);
        }
        public static void createMultipleChoiceCuve(DataTable dt, string xStr, string yStr)
        {
            ZedGraphControl zgc = new ZedGraphControl();
            GraphPane myPane = zgc.GraphPane;
            if (Utils.exam.Equals("会考"))
            {
                zgc.Width = 518;
                zgc.Height = 290;
            }
            else
            {
                zgc.Width = 531;
                zgc.Height = 291;
            }


            // Set the title and axis labels
            myPane.Title.Text = " ";
            myPane.XAxis.Title.Text = xStr;
            myPane.YAxis.Title.Text = YaxisTransfer(yStr);

            string[] xlabels = new string[dt.Rows.Count];
            for (int i = 0; i < dt.Rows.Count; i++)
                xlabels[i] = dt.Rows[i][0].ToString();
            List<double[]> data = new List<double[]>();
            for (int i = 1; i < dt.Columns.Count; i++)
            {
                double[] temp = new double[dt.Rows.Count];
                data.Add(temp);
            }
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                for (int j = 1; j < dt.Columns.Count; j++)
                {
                    data[j - 1][i] = Convert.ToDouble(dt.Rows[i][j]);
                }
            }
            myPane.XAxis.Type = AxisType.Text;
            for (int i = 0; i < data.Count; i++)
            {
                LineItem myCurve = myPane.AddCurve(dt.Columns[i + 1].ColumnName, null, data[i], mycolor[i], mySymbol[i]);
                myCurve.Line.IsSmooth = false;
                myCurve.Symbol.Size = 12;
            }
            myPane.XAxis.Scale.TextLabels = xlabels;

            myPane.XAxis.MajorTic.IsBetweenLabels = true;
            myPane.XAxis.Scale.FontSpec.Size = 16;
            myPane.XAxis.Title.FontSpec.Size = 18;
            myPane.YAxis.Scale.FontSpec.Size = 16;
            myPane.YAxis.Title.FontSpec.Size = 18;
            myPane.XAxis.Title.FontSpec.Family = "Times New Roman";
            myPane.YAxis.Title.FontSpec.Family = "Times New Roman";
            myPane.YAxis.Title.FontSpec.Angle = 90;

            myPane.YAxis.MajorGrid.IsVisible = true;
            myPane.XAxis.MajorGrid.IsVisible = true;
            myPane.XAxis.MajorGrid.DashOff = 0;
            myPane.XAxis.MajorGrid.Color = Color.LightSteelBlue;
            myPane.YAxis.MajorGrid.Color = Color.LightSteelBlue;
            myPane.YAxis.MinorGrid.IsVisible = true;
            myPane.YAxis.MinorGrid.Color = Color.LightSteelBlue;
            //myPane.YAxis.MajorTic.IsInside = true;
            myPane.YAxis.Scale.Max = 1.0;
            myPane.YAxis.Scale.MajorStep = 0.5;
            myPane.YAxis.MinorTic.IsAllTics = false;
            myPane.XAxis.MinorTic.IsAllTics = false;
            myPane.YAxis.MajorTic.IsOpposite = false;
            myPane.XAxis.MajorTic.IsOpposite = false;
            // Set the GraphPane title font size to 16
            myPane.Title.FontSpec.Size = 16;
            // Turn off the legend
            if (dt.Columns.Count > 2)
            {
                myPane.Legend.IsVisible = true;
                myPane.Legend.Position = LegendPos.BottomCenter;
                myPane.Legend.FontSpec.Size = 15;
            }
            else
                myPane.Legend.IsVisible = false;
            myPane.Title.IsVisible = true;
            myPane.Chart.Fill = new Fill(Color.White);
            zgc.AxisChange();

            Bitmap sourceBitmap = new Bitmap(zgc.Width, zgc.Height);
            zgc.DrawToBitmap(sourceBitmap, new Rectangle(0, 0, zgc.Width, zgc.Height));
            //Thread.CurrentThread.SetApartmentState(ApartmentState.STA);
            Clipboard.Clear();
            Clipboard.SetImage(sourceBitmap);
        }
        public static void createMultipleCuve(DataTable dt, string xStr, string yStr, double minX, double maxX)
        {
            ZedGraphControl zgc = new ZedGraphControl();
            GraphPane myPane = zgc.GraphPane;
            zgc.Width = 735;
            zgc.Height = 450;


            // Set the title and axis labels
            myPane.Title.Text = " ";
            myPane.XAxis.Title.Text = xStr;
            myPane.YAxis.Title.Text = YaxisTransfer(yStr);

            List<PointPairList> data = new List<PointPairList>();
            for (int i = 1; i < dt.Columns.Count; i++)
            {
                PointPairList temp = new PointPairList();
                data.Add(temp);
            }
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                for (int j = 1; j < dt.Columns.Count; j++)
                {
                    data[j - 1].Add(Convert.ToDouble(dt.Rows[i][0]), Convert.ToDouble(dt.Rows[i][j]), dt.Columns[j].ColumnName);
                }
            }
            for(int i = 0; i < data.Count; i++)
            {
                LineItem myCurve = myPane.AddCurve(dt.Columns[i + 1].ColumnName, data[i], mycolor[i]);
                myCurve.Line.Style = myDashStyle[i];
                myCurve.Line.Width = myWidth[i];
                myCurve.Line.IsSmooth = true;
                //myCurve.Line.SmoothTension = 1F;//
                myCurve.Symbol.Type = SymbolType.None;



                // Turn off the line, so the curve will by symbols only
                myCurve.Line.IsVisible = true;
            }

            myPane.XAxis.Scale.FontSpec.Size = 12;
            myPane.XAxis.Title.FontSpec.Size = 14;
            myPane.YAxis.Scale.FontSpec.Size = 12;
            myPane.YAxis.Title.FontSpec.Size = 14;
            myPane.XAxis.Title.FontSpec.Family = "宋体";
            myPane.YAxis.Title.FontSpec.Family = "宋体";
            myPane.YAxis.Title.FontSpec.Angle = 90;
            myPane.YAxis.Scale.MagAuto = false;

            myPane.YAxis.MinorTic.IsAllTics = false;
            myPane.XAxis.MinorTic.IsAllTics = false;
            myPane.YAxis.MajorTic.IsOpposite = false;
            myPane.XAxis.MajorTic.IsOpposite = false;
            if (yStr.Equals("难度"))
                myPane.YAxis.Scale.Min = 0;
            myPane.XAxis.Scale.Max = maxX;
            myPane.XAxis.Scale.Min = minX;
            myPane.XAxis.Scale.MajorStep = maxX / 10;
            //myPane.YAxis.Scale.Max = maxY;
            //myPane.YAxis.Scale.MajorStep = maxY / 2;
            if (yStr.Equals("比率(%)"))
                myPane.YAxis.Scale.Max = 100;
            // Set the GraphPane title font size to 16
            myPane.Title.FontSpec.Size = 16;
            // Turn off the legend
            myPane.Legend.IsVisible = true;
            myPane.Legend.Position = LegendPos.Right;
            myPane.Title.IsVisible = true;
            myPane.Chart.Fill = new Fill(Color.White);
            zgc.AxisChange();

            Bitmap sourceBitmap = new Bitmap(zgc.Width, zgc.Height);
            zgc.DrawToBitmap(sourceBitmap, new Rectangle(0, 0, zgc.Width, zgc.Height));
            //Thread.CurrentThread.SetApartmentState(ApartmentState.STA);
            Clipboard.Clear();
            Clipboard.SetImage(sourceBitmap);
        }
        public static void createCuve(string xStr, string yStr, double[][] cuveData, double minX, double maxX, double maxY)
        {

            ZedGraphControl zgc = new ZedGraphControl();
            GraphPane myPane = zgc.GraphPane;
            if (Utils.exam.Equals("会考"))
            {
                zgc.Width = 523;
                zgc.Height = 267;
            }
            else
            {
                zgc.Width = 531;
                zgc.Height = 271;
            }


            // Set the title and axis labels
            myPane.Title.Text = " ";
            myPane.XAxis.Title.Text = xStr;
            myPane.YAxis.Title.Text = YaxisTransfer(yStr);

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
            LineItem myCurve = myPane.AddCurve("", ppCurve, Color.Red);
            myCurve.Line.IsSmooth = true;
            //myCurve.Line.SmoothTension = 1F;//
            myCurve.Symbol.Type = SymbolType.None;



            // Turn off the line, so the curve will by symbols only
            myCurve.Line.IsVisible = true;



            // Set the x and y scale and title font sizes to 14
            myPane.XAxis.Scale.FontSpec.Size = 16;
            myPane.XAxis.Title.FontSpec.Size = 18;
            myPane.YAxis.Scale.FontSpec.Size = 16;
            myPane.YAxis.Title.FontSpec.Size = 18;
            myPane.XAxis.Title.FontSpec.Family = "宋体";
            myPane.YAxis.Title.FontSpec.Family = "宋体";
            myPane.YAxis.Title.FontSpec.Angle = 90;
            myPane.YAxis.Scale.MagAuto = false;

            myPane.YAxis.MinorTic.IsAllTics = false;
            myPane.XAxis.MinorTic.IsAllTics = false;
            myPane.YAxis.MajorTic.IsOpposite = false;
            myPane.XAxis.MajorTic.IsOpposite = false;
            myPane.XAxis.Scale.Max = maxX;
            myPane.XAxis.Scale.Min = minX;
            myPane.XAxis.Scale.MajorStep = maxX / 10;
            if (yStr.Equals("难度"))
                myPane.YAxis.Scale.Min = 0;
            myPane.YAxis.Scale.Max = maxY;
            myPane.YAxis.Scale.MajorStep = maxY / 2;

            // Set the GraphPane title font size to 16
            myPane.Title.FontSpec.Size = 16;
            // Turn off the legend
            myPane.Legend.IsVisible = false;
            myPane.Title.IsVisible = true;
            myPane.Chart.Fill = new Fill(Color.White);
            zgc.AxisChange();


            //添加一条线
            LineObj line = new LineObj(0.00, 0.5, maxX, 0.50);
            line.Line.Style = System.Drawing.Drawing2D.DashStyle.Custom;
            line.Line.DashOn = 10f;
            line.Line.DashOff = 8f;
            line.IsClippedToChartRect = true;
            line.Line.Color = Color.LightSteelBlue;
            line.ZOrder = ZOrder.F_BehindGrid;
            line.Location.AlignH = AlignH.Left;
            line.Location.AlignV = AlignV.Top;
            line.Location.CoordinateFrame = CoordType.AxisXYScale;
            myPane.GraphObjList.Add(line);
            

            if (yStr.Equals("难度") && Utils.GroupMark.Count > 0)
            {
                foreach (decimal temp in Utils.GroupMark)
                {
                    LineObj line1 = new LineObj(Convert.ToDouble(temp), 0.0, Convert.ToDouble(temp), maxY);
                    line1.Line.Style = System.Drawing.Drawing2D.DashStyle.Custom;
                    line1.Line.DashOn = 10f;
                    line1.Line.DashOff = 8f;
                    line1.IsClippedToChartRect = true;
                    line1.Line.Color = Color.LightSteelBlue;
                    line1.ZOrder = ZOrder.F_BehindGrid;
                    line1.Location.AlignH = AlignH.Left;
                    line1.Location.AlignV = AlignV.Top;
                    line1.Location.CoordinateFrame = CoordType.AxisXYScale;
                    myPane.GraphObjList.Add(line1);
                }
            }

            zgc.AxisChange();
            BarItem.CreateBarLabels(myPane, false, null);
            zgc.Refresh();

            Bitmap sourceBitmap = new Bitmap(zgc.Width, zgc.Height);
            zgc.DrawToBitmap(sourceBitmap, new Rectangle(0, 0, zgc.Width, zgc.Height));
            //Thread.CurrentThread.SetApartmentState(ApartmentState.STA);
            Clipboard.Clear();
            Clipboard.SetImage(sourceBitmap);
            //sourceBitmap.Save(cuveBmpPath + @"\testCuve.bmp");
        }
        public static void createGradient(double[][] data)
        {
            ZedGraphControl zgc = new ZedGraphControl();
            GraphPane myPane = zgc.GraphPane;
            zgc.Width = 531;
            zgc.Height = 271;

            // Set the title and axis labels
            myPane.Title.Text = " ";
            myPane.XAxis.Title.Text = "难度";
            myPane.YAxis.Title.Text = YaxisTransfer("区分度");

            // Enter some calculated data constants
            double[] x = new double[data.Length];
            double[] y = new double[data.Length];

            for (int i = 0; i < data.Length; i++)
            {
                x[i] = data[i][0];
                y[i] = data[i][1];

            }


            PointPairList pp = new PointPairList(x, y);


            // PointPairList pp = new PointPairList(y, x);

            // Generate a red curve with diamond symbols, and "Gas Data" in the legend
            LineItem myCurve = myPane.AddCurve("", pp, Color.Black,
                                        SymbolType.Square);
            myCurve.Symbol.Size = 8;
            // Set up a red-blue color gradient to be used for the fill
            //myCurve.Symbol.Fill = new Fill(Color.Red, Color.Blue);
            //myCurve.Symbol.Fill = new Fill(Color.Blue);
            // Turn off the symbol borders
            myCurve.Symbol.Border.IsVisible = true;
            // Instruct ZedGraph to fill the symbols by selecting a color out of the
            // red-blue gradient based on the Z value.  A value of 19 or less will be red,
            // a value of 34 or more will be blue, and values in between will be a
            // linearly apportioned color between red and blue.
            myCurve.Symbol.Fill.Type = FillType.GradientByZ;
            //myCurve.Symbol.Fill.SecondaryValueGradientColor = Color.Empty;
            //myCurve.Symbol.Fill.RangeMin = 0.2;
            //myCurve.Symbol.Fill.RangeMax = 0.8;
            //myCurve.Symbol.Fill.RangeDefault = 0.8;


            // Turn off the line, so the curve will by symbols only
            myCurve.Line.IsVisible = false;


            // Show the X and Y grids
            //myPane.XAxis.MajorGrid.IsVisible = true;
            //myPane.YAxis.MajorGrid.IsVisible = true;

            // Set the x and y scale and title font sizes to 14
            myPane.XAxis.Scale.FontSpec.Size = 16;
            myPane.XAxis.Title.FontSpec.Size = 18;
            myPane.YAxis.Scale.FontSpec.Size = 16;
            myPane.YAxis.Title.FontSpec.Size = 18;
            myPane.XAxis.Title.FontSpec.Family = "宋体";
            myPane.YAxis.Title.FontSpec.Family = "宋体";
            myPane.YAxis.Title.FontSpec.Angle = 90;
            //myPane.XAxis.Title.FontSpec.Fill.AlignH = AlignH.Left;
            //myPane.XAxis.Title.FontSpec.Fill.AlignV = AlignV.Top;

            //myPane.XAxis.Title.FontSpec.IsBold = false;

            myPane.XAxis.Scale.Max = 1.00;
            myPane.XAxis.Scale.Min = 0.00;
            myPane.XAxis.Scale.MajorStep = 0.20;
            myPane.YAxis.Scale.Max = 1.00;
            myPane.YAxis.Scale.MajorStep = 0.20;

            // Set the GraphPane title font size to 16
            myPane.Title.FontSpec.Size = 16;
            // Turn off the legend
            myPane.Legend.IsVisible = false;
            // Turn off the Title
            myPane.Title.IsVisible = true;
            // Fill the axis background with a color gradient
            //myPane.Chart.Fill = new Fill(Color.White, Color.FromArgb(255, 255, 166), 90F);
            myPane.Chart.Fill = new Fill(Color.White);
            zgc.AxisChange();
            myPane.YAxis.MinorTic.IsAllTics = false;
            myPane.XAxis.MinorTic.IsAllTics = false;
            myPane.YAxis.MajorTic.IsOpposite = false;
            myPane.XAxis.MajorTic.IsOpposite = false;

            //添加一条线
            LineObj line = new LineObj(0.20, 0, 0.20, 0.20);
            line.Line.Style = System.Drawing.Drawing2D.DashStyle.Custom;
            line.Line.DashOn = 10f;
            line.Line.DashOff = 8f;
            line.IsClippedToChartRect = true;
            line.Line.Color = Color.LightSteelBlue;
            line.ZOrder = ZOrder.F_BehindGrid;
            line.Location.AlignH = AlignH.Left;
            line.Location.AlignV = AlignV.Top;
            line.Location.CoordinateFrame = CoordType.AxisXYScale;
            myPane.GraphObjList.Add(line);

            //添加一条线
            LineObj line1 = new LineObj(0.80, 0, 0.80, 0.20);
            line1.Line.Style = System.Drawing.Drawing2D.DashStyle.Custom;
            line1.Line.DashOn = 10f;
            line1.Line.DashOff = 8f;
            line1.IsClippedToChartRect = true;
            line1.Line.Color = Color.LightSteelBlue;
            line1.ZOrder = ZOrder.F_BehindGrid;
            line1.Location.AlignH = AlignH.Left;
            line1.Location.AlignV = AlignV.Top;
            line1.Location.CoordinateFrame = CoordType.AxisXYScale;
            myPane.GraphObjList.Add(line1);

            //添加一条线
            double Xmax = myPane.XAxis.Scale.Max;
            LineObj line2 = new LineObj(0, 0.20, Xmax, 0.20);
            line2.Line.Style = System.Drawing.Drawing2D.DashStyle.Custom;
            line2.Line.DashOn = 10f;
            line2.Line.DashOff = 8f;
            line2.IsClippedToChartRect = true;
            line2.Line.Color = Color.LightSteelBlue;
            line2.ZOrder = ZOrder.F_BehindGrid;
            line2.Location.AlignH = AlignH.Left;
            line2.Location.AlignV = AlignV.Top;
            line2.Location.CoordinateFrame = CoordType.AxisXYScale;
            myPane.GraphObjList.Add(line2);

            BarItem.CreateBarLabels(myPane, false, null);
            zgc.Refresh();

            Bitmap sourceBitmap = new Bitmap(zgc.Width, zgc.Height);
            zgc.DrawToBitmap(sourceBitmap, new Rectangle(0, 0, zgc.Width, zgc.Height));
            //Thread.CurrentThread.SetApartmentState(ApartmentState.STA);
            Clipboard.Clear();
            Clipboard.SetImage(sourceBitmap);
            //sourceBitmap.Save(cuveBmpPath + @"\testCuve.bmp");

        }


        /// <summary>
        /// 折线图+柱状图的生成
        /// </summary>
        /// <param name="cuveBmpPath">图片的路径 不包括后缀 需要拼接</param>
        public static void createCuveAndBar(double[] cuveData, double[][] barData, double maxSource)
        {
            ZedGraphControl zgc = new ZedGraphControl();
            //图大小设置
            if (Utils.exam.Equals("会考"))
            {
                zgc.Width = 523;
                zgc.Height = 267;
            }
            else
            {
                zgc.Width = 531;
                zgc.Height = 271;
            }


            GraphPane myPane = zgc.GraphPane;
            //标题清空
            myPane.Title.Text = " ";
            myPane.XAxis.Title.Text = "";
            myPane.YAxis.Title.Text = "";
            //缓存清空
            myPane.CurveList.Clear();
            myPane.GraphObjList.Clear();
            //标题设置

            myPane.XAxis.Title.Text = "分数";
            myPane.YAxis.Title.Text = YaxisTransfer("人数");

            double pingjunzhi = cuveData[0];
            double biaozhuncha = cuveData[1];

            double[] cuveX = new double[(int)maxSource];
            double[] cuveY = new double[(int)maxSource];

            for (int i = 0; i < (int)maxSource; i++)
            {
                cuveX[i] = i;
                //cuveY[i] = cuveData[i][1];
                cuveY[i] = NormalCompute(i, pingjunzhi, biaozhuncha);
            }
            PointPairList ppCurve = new PointPairList(cuveX, cuveY);
            LineItem myCurve = myPane.AddCurve("", ppCurve, Color.Red);
            myCurve.Line.IsSmooth = true;
            //myCurve.Line.SmoothTension = 1F;//
            myCurve.Symbol.Type = SymbolType.None;

            myCurve.IsY2Axis = true;


            double[] barX = new double[barData.Length];
            double[] barY = new double[barData.Length];

            for (int i = 0; i < barData.Length; i++)
            {
                barX[i] = barData[i][0];
                barY[i] = barData[i][1];
            }
            PointPairList ppBar = new PointPairList(barX, barY);
            BarItem myCurve1 = myPane.AddBar("", ppBar, Color.FromArgb(0, 255, 255));

            myCurve1.Bar.Fill = new Fill(Color.FromArgb(0, 255, 255), Color.FromArgb(0, 255, 255));

            myPane.Legend.IsVisible = false;
            myPane.Title.IsVisible = true;

            myPane.YAxis.MinorTic.IsAllTics = false;
            myPane.XAxis.MinorTic.IsAllTics = false;
            myPane.YAxis.MajorTic.IsOpposite = false;
            myPane.XAxis.MajorTic.IsOpposite = false;
            //zgc.GraphPane.XAxis.Scale.Min = 1;//最小间隔
            //myPane.BarSettings.ClusterScaleWidthAuto = false;
            //myPane.BarSettings.ClusterScaleWidth = maxSource / barData.Length + 5;
            //myPane.BarSettings.MinBarGap = 0.1f;
            myPane.BarSettings.MinClusterGap = 0.1f;
            myPane.XAxis.Scale.FontSpec.Size = 16;
            myPane.XAxis.Title.FontSpec.Size = 18;
            myPane.YAxis.Scale.FontSpec.Size = 16;
            myPane.YAxis.Title.FontSpec.Size = 18;
            myPane.XAxis.Title.FontSpec.Family = "宋体";
            myPane.YAxis.Title.FontSpec.Family = "宋体";
            myPane.YAxis.Title.FontSpec.Angle = 90;
            myPane.YAxis.Title.IsTitleAtCross = false;
            myPane.XAxis.Scale.Max = maxSource + 0.5;
            myPane.XAxis.Scale.Min = barX[0] - 0.5;
            myPane.YAxis.Scale.MagAuto = false;
            //if (Utils.exam.Equals("会考"))
            //{
            //    TextObj mark = new TextObj("this is a note", 0.7F, 0.95F);
            //    mark.Location.CoordinateFrame = CoordType.PaneFraction;
            //    mark.FontSpec.FontColor = Color.Blue;
            //    mark.FontSpec.Size = 10F;

            //    myPane.GraphObjList.Add(mark);
            //}
            zgc.AxisChange();
            zgc.Refresh();

            Bitmap sourceBitmap = new Bitmap(zgc.Width, zgc.Height);
            zgc.DrawToBitmap(sourceBitmap, new Rectangle(0, 0, zgc.Width, zgc.Height));
            //Thread.CurrentThread.SetApartmentState(ApartmentState.STA);
            Clipboard.Clear();
            Clipboard.SetImage(sourceBitmap);
            //sourceBitmap.Save(cuveBmpPath + @"\testCuve.bmp");
        }

        /// <summary>
        /// 柱状图的生成
        /// </summary>
        /// <param name="cuveBmpPath"></param>
        /// <param name="barData"></param>
        public static void createBar(double[][] barData)
        {
            ZedGraphControl zgc = new ZedGraphControl();
            //图大小设置
            zgc.Width = 531;
            zgc.Height = 271;


            GraphPane myPane = zgc.GraphPane;
            //标题清空
            myPane.Title.Text = "";
            myPane.XAxis.Title.Text = "";
            myPane.YAxis.Title.Text = "";
            //缓存清空
            myPane.CurveList.Clear();
            myPane.GraphObjList.Clear();

            //标题设置

            myPane.XAxis.Title.Text = "分数";
            myPane.YAxis.Title.Text = YaxisTransfer("人数");
            myPane.YAxis.Title.FontSpec.Angle = 90;

            double[] barX = new double[barData.Length];
            double[] barY = new double[barData.Length];

            for (int i = 0; i < barData.Length; i++)
            {
                barX[i] = barData[i][0];
                barY[i] = barData[i][1];
            }
            PointPairList ppBar = new PointPairList(barX, barY);
            BarItem myCurve1 = myPane.AddBar("", ppBar, Color.FromArgb(0, 255, 255));

            myCurve1.Bar.Fill = new Fill(Color.FromArgb(0, 255, 255), Color.FromArgb(0, 255, 255));

            myPane.Legend.IsVisible = false;
            myPane.Title.IsVisible = true;

            myPane.YAxis.MinorTic.IsAllTics = false;
            myPane.XAxis.MinorTic.IsAllTics = false;
            myPane.YAxis.MajorTic.IsOpposite = false;
            myPane.XAxis.MajorTic.IsOpposite = false;
            myPane.XAxis.Scale.FontSpec.Size = 16;
            myPane.XAxis.Title.FontSpec.Size = 18;
            myPane.YAxis.Scale.FontSpec.Size = 16;
            myPane.YAxis.Title.FontSpec.Size = 18;
            myPane.XAxis.Title.FontSpec.Family = "宋体";
            myPane.YAxis.Title.FontSpec.Family = "宋体";
            myPane.YAxis.Scale.MagAuto = false;
            double max;
            double min;
            if (Math.Floor(barX[barData.Length - 1]) == barX[barData.Length - 1])
                max = barX[barData.Length - 1] + 0.5;
            else
                max = barX[barData.Length - 1] + 1.5;
            if (Math.Floor(barX[0]) == barX[0])
                min = barX[0] - 0.5;
            else
                min = barX[0] - 1.5;
            myPane.XAxis.Scale.Max = max;
            myPane.XAxis.Scale.Min = min;
            //myPane.XAxis.Scale.Max = barX[barX.Length - 1] + 20;

            //myPane.XAxis.Scale.Max = barData[barData.Length - 1][0] + 1;
            //myPane.XAxis.Scale.Min = barData[0][0] - 1;
            
            zgc.AxisChange();
            zgc.Refresh();

            Bitmap sourceBitmap = new Bitmap(zgc.Width, zgc.Height);
            zgc.DrawToBitmap(sourceBitmap, new Rectangle(0, 0, zgc.Width, zgc.Height));
            //Thread.CurrentThread.SetApartmentState(ApartmentState.STA);
            Clipboard.Clear();
            Clipboard.SetImage(sourceBitmap);
            //sourceBitmap.Save(cuveBmpPath + @"\testCuve.bmp");
        }


        public static double NormalCompute(double x, double pingjunzhi, double biaozhuncha)
        {
            double SQRT_INV = 1.0 / System.Math.Sqrt(2.0 * System.Math.PI * biaozhuncha * biaozhuncha);
            double diff = x - pingjunzhi;
            return SQRT_INV * System.Math.Exp((-(diff * diff)) / (2.0 * biaozhuncha * biaozhuncha));
        }

        public static string YaxisTransfer(string name)
        {
            char[] names = name.ToCharArray();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < names.Length; i++ )
            {
                if (names[i].Equals('（') || names[i].Equals('('))
                {
                    while (i < names.Length)
                    {
                        if (names[i].Equals('）') || names[i].Equals(')'))
                        {
                            sb.Append(names[i]);
                            break;
                        }
                        sb.Append(name[i]);
                        i++;
                    }
                    sb.Append("\n");
                }
                else
                {
                    sb.Append(names[i]);
                    sb.Append("\n");
                }

            }
            return sb.ToString();
        }
    }
}
