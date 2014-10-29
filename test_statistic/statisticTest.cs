using ExamReport;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;
using System.Data.OleDb;

namespace test_statistic
{
    
    
    /// <summary>
    ///This is a test class for statisticTest and is intended
    ///to contain all statisticTest Unit Tests
    ///</summary>
    [TestClass()]
    public class statisticTest
    {


        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        //
        //Use ClassInitialize to run code before running the first test in the class
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Use ClassCleanup to run code after all tests in a class have run
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        //
        //Use TestInitialize to run code before running each test
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //}
        //
        //Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion


        /// <summary>
        ///A test for statistic_process
        ///</summary>
        [TestMethod()]
        public void statistic_processTest()
        {
            string strConn = @"Provider=vfpoledb;Data Source=D:\项目\给王卅的编程资料\数据库\中考学科数据\wl;Collating Sequence=machine;";
            OleDbConnection myConnection = new OleDbConnection(strConn);
            
                myConnection.Open();
                OleDbDataAdapter da = new OleDbDataAdapter("select * from wl_full", myConnection);
                DataSet ds = new DataSet();
                da.Fill(ds);
                DataTable dt = ds.Tables[0];

                OleDbDataAdapter da_groups = new OleDbDataAdapter("select * from wl_groups", myConnection);
                DataSet ds_groups = new DataSet();
                da_groups.Fill(ds_groups);
                DataTable groups_table = ds_groups.Tables[0];

                excel_process answer_excel = new excel_process(@"D:\项目\给王卅的编程资料\数据库\中考学科数据\wl\ans.xlsx");
                answer_excel.run(true);
                answer_excel.KillSpecialExcel();
                excel_process groups_excel = new excel_process(@"D:\项目\给王卅的编程资料\数据库\中考学科数据\wl\groups.xlsx");
                groups_excel.run(false);
                groups_excel.KillSpecialExcel();
                //DataTable dt = null; // TODO: Initialize to an appropriate value
                Decimal fullmark = 100.0m; // TODO: Initialize to an appropriate value
                DataTable standard_ans = answer_excel.dt; // TODO: Initialize to an appropriate value
                int num = 16; // TODO: Initialize to an appropriate value
                //DataTable groups_table = null; // TODO: Initialize to an appropriate value
                DataTable groups_ans = groups_excel.dt; // TODO: Initialize to an appropriate value
                //Total_statistic target = new Total_statistic(dt, fullmark, standard_ans, num, groups_table, groups_ans, 7); // TODO: Initialize to an appropriate value

            
            
                bool expected = false; // TODO: Initialize to an appropriate value
                bool actual;
                //actual = target.statistic_process();
                //WordCreator create = new WordCreator(target.result);
                ////create.creating_word();
                //Assert.AreEqual(expected, actual);
                //Assert.Inconclusive("Verify the correctness of this test method.");
            
        }
    }
}
