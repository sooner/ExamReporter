using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Threading;
using System.Configuration;

using System.Drawing.Drawing2D;

namespace ExamReport
{
    public delegate void MyDelegate(int i, int status);
    public delegate void ErrorMessage(string Message);
    public partial class Form1 : Form
    {
        SchoolCodeConfig schoolcode;

        Thread thread;
         
        public Form1()
        {
            InitializeComponent();

            Zonghe_disable();
            Quxian_disable();
            SFX_disable();
            CJ_disable();
            groupBox3.Enabled = false;
            popu_num.Enabled = true;
            Popu_choice.Select();
            remark_num.Enabled = false;

            excellent_high.Value = 100m;
            excellent_low.Value = 85m;
            well_high.Value = 85m;
            well_low.Value = 70m;
            pass_high.Value = 70m;
            pass_low.Value = 60m;
            fail_high.Value = 60m;
            fail_low.Value = 0m;
            cancel.Enabled = false;
            save_address.Text = System.IO.Path.GetDirectoryName(System.Windows.Forms.Application.ExecutablePath);

            groupBox5.Enabled = false;

            schoolcode = (SchoolCodeConfig)ConfigurationManager.GetSection("SchoolCode");

        }
        public void ErrorM(string message)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new ErrorMessage(ErrorM), message);
            }
            else
            {
                Error(message);
                if (thread.IsAlive)
                {
                    thread.Abort();
                    ShowPro(100, 5);
                }

            }
        }
        public void ShowPro(int value, int status)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new MyDelegate(ShowPro), value, status);
            }
            else
            {
                this.progressBar1.Value = value;
                switch (status)
                {
                    case 0:
                        run_button.Enabled = false;
                        cancel.Enabled = true;
                        this.label38.Text = "标准答案处理中...";
                        break;
                    case 1:
                        this.label38.Text = "分组信息处理中...";
                        break;
                    case 2:
                        this.label38.Text = "数据文件读入处理中...";
                        break;
                    case 3:
                        this.label38.Text = "数据处理中...";
                        break;
                    case 4:
                        this.label38.Text = "文档生成中...";
                        break;
                    case 5:
                        this.label38.Text = "完成！";
                        run_button.Enabled = true;
                        cancel.Enabled = false;
                        break;
                    default:
                        break;
                }
                
                
            }
        }

        

        private void comboBox1_SelectedIndexChanged_1(object sender, EventArgs e)
        {
            switch (exam.SelectedIndex)
            {
                case 0:
                    Quxian_disable();
                    Zonghe_disable();
                    SFX_disable();
                    CJ_disable();
                    deleteQX();
                    QX_list.ResetText();
                    groupBox3.Enabled = false;
                    subject.Items.Clear();
                    subject.Items.Add("语文");
                    subject.Items.Add("数学");
                    subject.Items.Add("英语");
                    subject.Items.Add("物理");
                    subject.Items.Add("化学");
                    subject.ResetText();

                    report.Items.Clear();
                    report.Items.Add("总体");
                    report.Items.Add("区县");
                    report.ResetText();

                    school.Items.Clear();
                    school.ResetText();
                    break;
                case 1:
                    Quxian_disable();
                    Zonghe_disable();
                    SFX_disable();
                    CJ_disable();
                    deleteQX();
                    QX_list.ResetText();
                    groupBox3.Enabled = true;
                    subject.Items.Clear();
                    subject.Items.Add("语文");
                    subject.Items.Add("数学");
                    subject.Items.Add("英语");
                    subject.Items.Add("物理");
                    subject.Items.Add("化学");
                    subject.Items.Add("生物");
                    subject.Items.Add("政治");
                    subject.Items.Add("历史");
                    subject.Items.Add("地理");
                    subject.ResetText();

                    report.Items.Clear();
                    report.ResetText();

                    school.Items.Clear();
                    school.ResetText();
                    break;
                case 2:
                    Quxian_disable();
                    Zonghe_disable();
                    SFX_disable();
                    CJ_disable();
                    deleteQX();
                    QX_list.ResetText();
                    groupBox3.Enabled = false;
                    subject.Items.Clear();
                    subject.Items.Add("总分");
                    subject.Items.Add("语文");
                    subject.Items.Add("英语");
                    subject.Items.Add("数学理");
                    subject.Items.Add("数学文");
                    subject.Items.Add("理综-物理");
                    subject.Items.Add("理综-化学");
                    subject.Items.Add("理综-生物");
                    subject.Items.Add("文综-政治");
                    subject.Items.Add("文综-地理");
                    subject.Items.Add("文综-历史");
                    subject.ResetText();

                    report.Items.Clear();
                    report.Items.Add("总体");
                    report.Items.Add("区县");
                    report.Items.Add("两类示范校");
                    report.Items.Add("城郊");
                    report.Items.Add("学校");
                    report.ResetText();

                    school.Items.Clear();

                    school.ResetText();
                    break;
                default:
                    break;

            }
        }

        private void Zonghe_disable()
        {
            label35.Enabled = false;
            WLZ_address.Enabled = false;
            button10.Enabled = false;
        }
        private void Zonghe_enable()
        {
            label35.Enabled = true;
            WLZ_address.Enabled = true;
            button10.Enabled = true;
        }

        private void Quxian_disable()
        {
            label15.Enabled = false;
            QXS_address.Enabled = false;
            button4.Enabled = false;
        }

        private void Quxian_enable()
        {
            label15.Enabled = true;
            QXS_address.Enabled = true;
            button4.Enabled = true;
        }
        private void SFX_enable()
        {
            label36.Enabled = true;
            SFX_address.Enabled = true;
            button11.Enabled = true;

        }

        private void SFX_disable()
        {
            label36.Enabled = false;
            SFX_address.Enabled = false;
            button11.Enabled = false;
        }

        private void CJ_disable()
        {
            label37.Enabled = false;
            CJ_address.Enabled = false;
            button9.Enabled = false;
        }
        private void CJ_enable()
        {
            label37.Enabled = true;
            CJ_address.Enabled = true;
            button9.Enabled = true;
        }
        private void zongfen_enable()
        {
            standard_ans.Enabled = true;
            groups_address.Enabled = true;
            button2.Enabled = true;
            button3.Enabled = true;
            Popu_choice.Enabled = true;
            Mark_choice.Enabled = true;
            popu_num.Enabled = true;
            remark_num.Enabled = true;
        }
        private void zongfen_disable()
        {
            standard_ans.Enabled = false;
            groups_address.Enabled = false;
            button2.Enabled = false;
            button3.Enabled = false;
            Popu_choice.Enabled = false;
            Mark_choice.Enabled = false;
            popu_num.Enabled = false;
            remark_num.Enabled = false;
        }
        private void comboBox2_SelectedIndexChanged(object sender, EventArgs e)
        {

            if (subject.SelectedItem.ToString().Trim().Contains("理综") ||
                subject.SelectedItem.ToString().Trim().Contains("文综"))
            {
                groupBox5.Enabled = true;
                Zonghe_enable();
                zongfen_enable();
                if (subject.SelectedItem.ToString().Trim().Contains("理综"))
                {
                    label41.Text = "生物:";
                    label42.Text = "物理:";
                    label43.Text = "化学:";
                }
                else
                {
                    label41.Text = "政治:";
                    label42.Text = "历史:";
                    label43.Text = "地理:";
                }
            }
            else if (subject.SelectedItem.ToString().Trim().Equals("总分"))
            {
                groupBox5.Enabled = false;
                Zonghe_disable();
                zongfen_disable();
            }
            else
            {
                groupBox5.Enabled = false;
                Zonghe_disable();
                zongfen_enable();
            }
        }
        void AddQX()
        {
            SchoolCodeConfig schoolcode = (SchoolCodeConfig)ConfigurationManager.GetSection("DistrictCode");
            QX_list.DataSource = schoolcode.KeyValues.Cast<MyKeyValueSetting>().ToDataTable();
            QX_list.DisplayMember = "value";
            QX_list.ValueMember = "key";              
            
            QX_list.ResetText();
        }
        void deleteQX()
        {
            QX_list.DataSource = null;
            QX_list.ResetText();
        }
        private void comboBox3_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (report.SelectedItem.ToString().Trim().Equals("区县"))
            {
                Quxian_enable();
                CJ_enable();
                if(exam.SelectedItem.ToString().Trim().Equals("高考"))
                    SFX_enable();
                AddQX();
                
            }
            else if (report.SelectedItem.ToString().Trim().Equals("两类示范校"))
            {
                Quxian_disable();
                SFX_enable();
                CJ_disable();
                deleteQX();
            }
            else if (report.SelectedItem.ToString().Trim().Equals("城郊"))
            {
                Quxian_disable();
                CJ_enable();
                SFX_disable();
                deleteQX();
            }
            else if (report.SelectedItem.ToString().Trim().Equals("学校"))
            {
                Quxian_enable();
                CJ_enable();
                SFX_enable();
                AddQX();
            }
            else
            {
                Quxian_disable();
                SFX_disable();
                CJ_disable();
                deleteQX();
            }

            
        }

        private void button1_Click_1(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();
            openFileDialog1.InitialDirectory = "C://";
            openFileDialog1.Filter = "DBF files (*.dbf)|*.dbf|All files (*.*)|*.*";
            openFileDialog1.FilterIndex = 1;
            openFileDialog1.RestoreDirectory = true;
            if (openFileDialog1.ShowDialog() == DialogResult.OK)
                data_address.Text = openFileDialog1.FileName;
        }

        private void button2_Click_1(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();
            openFileDialog1.InitialDirectory = "C://";
            openFileDialog1.Filter = "Excel files (*.xls,*.xlsx)|*.xls;*.xlsx|All files (*.*)|*.*";
            openFileDialog1.FilterIndex = 1;
            openFileDialog1.RestoreDirectory = true;
            if (openFileDialog1.ShowDialog() == DialogResult.OK)
                standard_ans.Text = openFileDialog1.FileName;
        }

        private void button3_Click_1(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();
            openFileDialog1.InitialDirectory = "C://";
            openFileDialog1.Filter = "Excel files (*.xls,*.xlsx)|*.xls;*.xlsx|All files (*.*)|*.*";
            openFileDialog1.FilterIndex = 1;
            openFileDialog1.RestoreDirectory = true;
            if (openFileDialog1.ShowDialog() == DialogResult.OK)
                groups_address.Text = openFileDialog1.FileName;
        }

        private void button10_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();
            openFileDialog1.InitialDirectory = "C://";
            openFileDialog1.Filter = "Excel files (*.xls,*.xlsx)|*.xls;*.xlsx|All files (*.*)|*.*";
            openFileDialog1.FilterIndex = 1;
            openFileDialog1.RestoreDirectory = true;
            if (openFileDialog1.ShowDialog() == DialogResult.OK)
                WLZ_address.Text = openFileDialog1.FileName;
        }

        private void button4_Click_1(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();
            openFileDialog1.InitialDirectory = "C://";
            openFileDialog1.Filter = "Excel files (*.xls,*.xlsx)|*.xls;*.xlsx|All files (*.*)|*.*";
            openFileDialog1.FilterIndex = 1;
            openFileDialog1.RestoreDirectory = true;
            if (openFileDialog1.ShowDialog() == DialogResult.OK)
                QXS_address.Text = openFileDialog1.FileName;
        }

        private void button11_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();
            openFileDialog1.InitialDirectory = "C://";
            openFileDialog1.Filter = "Excel files (*.xls,*.xlsx)|*.xls;*.xlsx|All files (*.*)|*.*";
            openFileDialog1.FilterIndex = 1;
            openFileDialog1.RestoreDirectory = true;
            if (openFileDialog1.ShowDialog() == DialogResult.OK)
                SFX_address.Text = openFileDialog1.FileName;
        }

        

        private void radioButton3_CheckedChanged(object sender, EventArgs e)
        {
            RadioButton rb = (RadioButton)sender;
            if (rb.Checked)
            {
                popu_num.Enabled = true;
            }
            else
            {
                popu_num.Enabled = false;
            }
        }

        private void radioButton4_CheckedChanged(object sender, EventArgs e)
        {
            RadioButton rb = (RadioButton)sender;
            if (rb.Checked)
            {
                remark_num.Enabled = true;
            }
            else
            {
                remark_num.Enabled = false;
            }
        }

        private bool Error(string errormessage)
        {
            MessageBox.Show(errormessage, "提示", MessageBoxButtons.OK, MessageBoxIcon.Error);
            return false;
        }

        private void button9_Click(object sender, EventArgs e)
        {
            
            start_process();
        }
        void start_process()
        {
            if (!ConfigCheck())
                return;
            startProcess start = new startProcess(this);
            ExecuteMethod exe = start.exe;
            if (checkBox1.Checked)
                Utils.isVisible = true;
            else
                Utils.isVisible = false;
            if (checkBox2.Checked)
                Utils.saveMidData = true;
            else
                Utils.saveMidData = false;
            if (sub_iszero.Checked)
                Utils.sub_iszero = true;
            else
                Utils.sub_iszero = false;
            if (obj_iszero.Checked)
                Utils.obj_iszero = true;
            else
                Utils.obj_iszero = false;
            Utils.save_address = save_address.Text;
            Utils.fullmark = fullmark.Value;
            exe.Subject = subject.SelectedItem.ToString().Trim();
            if (!exam.SelectedItem.ToString().Trim().Equals("会考"))
                exe.Report_style = report.SelectedItem.ToString().Trim();
            exe.Database_address = data_address.Text;
            exe.Ans_address = standard_ans.Text;
            exe.Groups_address = groups_address.Text;
            if (Popu_choice.Checked)
            {
                exe.grouptype = ZK_database.GroupType.population;
                exe.divider = popu_num.Value;
            }
            if (Mark_choice.Checked)
            {
                exe.grouptype = ZK_database.GroupType.totalmark;
                exe.divider = remark_num.Value;

            }
            exe.fullmark = fullmark.Value;
            if (!exam.SelectedItem.ToString().Trim().Equals("会考"))
            {
                if (exe.Report_style.Equals("区县"))
                {
                    if (string.IsNullOrEmpty(QXS_address.Text.Trim()))
                    {
                        Error("请输入区县学校分类文件地址！");
                        return;
                    }
                    if (exam.SelectedItem.ToString().Trim().Equals("高考"))
                    {
                        if (string.IsNullOrEmpty(SFX_address.Text.Trim()))
                        {
                            Error("请输入示范学校分类文件地址！");
                            return;
                        }
                        exe.Shifan_catagory = SFX_address.Text;
                    }

                    if (QX_list.SelectedItem == null)
                    {
                        Error("请选择需要生成报告的区县！");
                        return;
                    }
                    if (string.IsNullOrEmpty(CJ_address.Text.Trim()))
                    {
                        Error("请输入城郊分类文件地址！");
                        return;
                    }
                    exe.Quxian_catagory = QXS_address.Text;

                    exe.Cj_catagory = CJ_address.Text;
                    exe.Quxian_list = QX_list.SelectedValue.ToString().Trim();
                    Utils.QX = QX_list.SelectedItem.ToString();
                }
                if (exe.Report_style.Equals("两类示范校"))
                {
                    if (string.IsNullOrEmpty(SFX_address.Text.Trim()))
                    {
                        Error("请输入示范学校分类文件地址！");
                        return;
                    }
                    exe.Shifan_catagory = SFX_address.Text;
                }
                if (exe.Report_style.Equals("城郊"))
                {
                    if (string.IsNullOrEmpty(CJ_address.Text.Trim()))
                    {
                        Error("请输入城郊分类文件地址！");
                        return;
                    }
                    exe.Cj_catagory = CJ_address.Text;
                }
                if (exe.Subject.Contains("文综") || exe.Subject.Contains("理综"))
                {
                    if (string.IsNullOrEmpty(WLZ_address.Text.Trim()))
                    {
                        Error("请输入文理综题目分类文件地址！");
                        return;
                    }
                    if (Math.Abs(wuli_lishi.Value) != wuli_lishi.Value ||
                        Math.Abs(shengwu_zhengzhi.Value) != shengwu_zhengzhi.Value ||
                        Math.Abs(huaxue_dili.Value) != huaxue_dili.Value)
                    { 
                        Error("综合总分不能为负！");
                        return;
                    }
                    if ((wuli_lishi.Value + shengwu_zhengzhi.Value + huaxue_dili.Value) != fullmark.Value)
                    {
                        Error("综合各科成绩的和应该等于总成绩！");
                        return;
                    }
                    Utils.shengwu_zhengzhi = shengwu_zhengzhi.Value;
                    Utils.wuli_lishi = wuli_lishi.Value;
                    Utils.huaxue_dili = huaxue_dili.Value;
                    exe.Wenli_catagory = WLZ_address.Text;
                }
            }
            if (exam.SelectedItem.ToString().Trim().Equals("中考"))
            {
                exe.Style = "中考";
            }
            else if (exam.SelectedItem.ToString().Trim().Equals("会考"))
            {
                exe.Style = "会考";
                if (!hk_check())
                    return;
                exe.hk_hierarchy = new ExecuteMethod.HK_hierarchy();
                exe.hk_hierarchy.excellent_low = excellent_low.Value;
                exe.hk_hierarchy.excellent_high = excellent_high.Value;
                exe.hk_hierarchy.well_low = well_low.Value;
                exe.hk_hierarchy.well_high = well_high.Value;
                exe.hk_hierarchy.pass_low = pass_low.Value;
                exe.hk_hierarchy.pass_high = pass_high.Value;
                exe.hk_hierarchy.fail_low = fail_low.Value;
                exe.hk_hierarchy.fail_high = fail_high.Value;
            }
            else if (exam.SelectedItem.ToString().Trim().Equals("高考"))
            {
                exe.Style = "高考";
            }
            else
                Error("怎么可能到这里的！");


            thread = new Thread(new ThreadStart(start.data_process));
            thread.IsBackground = true;
            thread.SetApartmentState(ApartmentState.STA);
            thread.Start();
        }
        private bool hk_check()
        {
            if (Math.Abs(excellent_low.Value) != excellent_low.Value ||
                Math.Abs(excellent_high.Value) != excellent_high.Value ||
                Math.Abs(well_low.Value) != well_low.Value ||
                Math.Abs(well_high.Value) != well_high.Value ||
                Math.Abs(pass_low.Value) != pass_low.Value ||
                Math.Abs(pass_high.Value) != pass_high.Value ||
                Math.Abs(fail_low.Value) != fail_low.Value ||
                Math.Abs(fail_high.Value) != fail_high.Value)
                return Error("会考成绩区域不能为负！");
            if (!(fail_low.Value < fail_high.Value &&
                pass_low.Value < pass_high.Value &&
                well_low.Value < well_high.Value &&
                excellent_low.Value < excellent_high.Value))
                return Error("会考成绩设置错误！");
            return true;
        }

        private bool ConfigCheck()
        {
            if (exam.SelectedItem == null)
                return Error("请选择考试类型！");
            if (subject.SelectedItem == null)
                return Error("请选择科目！");
            if (!exam.SelectedItem.ToString().Trim().Equals("会考"))
            {
                if (report.SelectedItem == null)
                    return Error("请选择报告类别！");
            }
            if (string.IsNullOrEmpty(data_address.Text.Trim()))
                return Error("请选择数据文件地址！");
            if (!subject.SelectedItem.ToString().Trim().Equals("总分"))
            {
                if (string.IsNullOrEmpty(standard_ans.Text.Trim()))
                    return Error("请选择标准答案文件地址！");
                if (string.IsNullOrEmpty(groups_address.Text.Trim()))
                    return Error("请选择分组文件地址！");
                if (!(Popu_choice.Checked || Mark_choice.Checked))
                    return Error("请选择分组类型！");
                if (Popu_choice.Checked && (Math.Abs(Math.Floor(popu_num.Value)) != popu_num.Value || popu_num.Value == 0))
                    return Error("组数应为非负整数！");
                if (Mark_choice.Checked && (Math.Abs(remark_num.Value) != remark_num.Value || remark_num.Value == 0))
                    return Error("分组分数应为非负数！");
            }
            if (Math.Abs(fullmark.Value) != fullmark.Value || fullmark.Value == 0)
                return Error("科目总分应为非负数");
            return true;
        }

        public class startProcess
        {
            public ExecuteMethod exe;
            public startProcess(Form1 _form)
            {
                exe = new ExecuteMethod();
                exe.form = _form;
            }
            public void data_process()
            {
                exe.form.ShowPro(0, 0);
                exe.pre_process();
                exe.form.ShowPro(100, 5);
            }
        }

        private void button9_Click_1(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();
            openFileDialog1.InitialDirectory = "C://";
            openFileDialog1.Filter = "Excel files (*.xls,*.xlsx)|*.xls;*.xlsx|All files (*.*)|*.*";
            openFileDialog1.FilterIndex = 1;
            openFileDialog1.RestoreDirectory = true;
            if (openFileDialog1.ShowDialog() == DialogResult.OK)
                CJ_address.Text = openFileDialog1.FileName;
        }

        private void button12_Click(object sender, EventArgs e)
        {
            FolderBrowserDialog openFolder = new FolderBrowserDialog();
            openFolder.ShowNewFolderButton = true;
            openFolder.Description = "保存至";
            if (openFolder.ShowDialog() == DialogResult.OK)
                save_address.Text = openFolder.SelectedPath;
        }

        private void cancel_Click(object sender, EventArgs e)
        {
            if (thread.IsAlive)
            {
                thread.Abort();
                ShowPro(100, 5);
                cancel.Enabled = false;
                run_button.Enabled = true;
            }

        }

        private void QX_list_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (QX_list.DataSource != null)
            {
                
                school.DataSource = (from kv in schoolcode.KeyValues.Cast<MyKeyValueSetting>()
                                     where kv.Key.StartsWith(QX_list.SelectedValue.ToString())
                                     let s = string.Format("{0} {1}", kv.Key, kv.Value)
                                     select s).ToArray();

                school.ResetText();
            }
            else
            {
                school.DataSource = null;
                school.ResetText();
            }

        }


        
        
        




        

        

        


     
        
    }
}
