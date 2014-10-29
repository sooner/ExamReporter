using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;
using System.IO;

namespace ExamReport
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            Utils.template_address = "";
            Application.EnableVisualStyles();
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(UnhandledExceptionEventHandler);
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new Form1());

        }
        static void UnhandledExceptionEventHandler(object sender, UnhandledExceptionEventArgs e)
        {
            try
            {
                File.WriteAllText(@Utils.CurrentDirectory + @"\err.log", e.ExceptionObject.ToString());//LogHelper是写日志的类，这里，可以直接写到文件里
            }
            catch
            {
            }
        }
    }
}
