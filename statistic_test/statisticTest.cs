using ExamReport;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;
using System.Data.OleDb;

namespace statistic_test
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
            using (OleDbConnection myConnection = new OleDbConnection(strConn))
            {
                myConnection.Open();
                OleDbDataAdapter da = new OleDbDataAdapter("select * from wl_full", myConnection);
                DataSet ds = new DataSet();
                da.Fill(ds);
                DataTable dt = ds.Tables[0]; // TODO: Initialize to an appropriate value
                double fullmark = 100.0; // TODO: Initialize to an appropriate value
                DataTable standard_ans = null; // TODO: Initialize to an appropriate value
                int num = 0; // TODO: Initialize to an appropriate value
                DataTable groups_table = null; // TODO: Initialize to an appropriate value
                DataTable groups_ans = null; // TODO: Initialize to an appropriate value
                statistic target = new statistic(dt, fullmark, standard_ans, num, groups_table, groups_ans); // TODO: Initialize to an appropriate value
                bool expected = false; // TODO: Initialize to an appropriate value
                bool actual;
                actual = target.statistic_process();
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }
        }

        /// <summary>
        ///A test for statistic Constructor
        ///</summary>
        [TestMethod()]
        public void statisticConstructorTest()
        {
            DataTable dt = null; // TODO: Initialize to an appropriate value
            double fullmark = 0F; // TODO: Initialize to an appropriate value
            DataTable standard_ans = null; // TODO: Initialize to an appropriate value
            int num = 0; // TODO: Initialize to an appropriate value
            DataTable groups_table = null; // TODO: Initialize to an appropriate value
            DataTable groups_ans = null; // TODO: Initialize to an appropriate value
            statistic target = new statistic(dt, fullmark, standard_ans, num, groups_table, groups_ans);
            Assert.Inconclusive("TODO: Implement code to verify target");
        }

        /// <summary>
        ///A test for statistic_process
        ///</summary>
        [TestMethod()]
        public void statistic_processTest1()
        {
            DataTable dt = null; // TODO: Initialize to an appropriate value
            double fullmark = 0F; // TODO: Initialize to an appropriate value
            DataTable standard_ans = null; // TODO: Initialize to an appropriate value
            int num = 0; // TODO: Initialize to an appropriate value
            DataTable groups_table = null; // TODO: Initialize to an appropriate value
            DataTable groups_ans = null; // TODO: Initialize to an appropriate value
            statistic target = new statistic(dt, fullmark, standard_ans, num, groups_table, groups_ans); // TODO: Initialize to an appropriate value
            bool expected = false; // TODO: Initialize to an appropriate value
            bool actual;
            actual = target.statistic_process();
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for statistic Constructor
        ///</summary>
        [TestMethod()]
        public void statisticConstructorTest1()
        {
            DataTable dt = null; // TODO: Initialize to an appropriate value
            double fullmark = 0F; // TODO: Initialize to an appropriate value
            DataTable standard_ans = null; // TODO: Initialize to an appropriate value
            int num = 0; // TODO: Initialize to an appropriate value
            DataTable groups_table = null; // TODO: Initialize to an appropriate value
            DataTable groups_ans = null; // TODO: Initialize to an appropriate value
            statistic target = new statistic(dt, fullmark, standard_ans, num, groups_table, groups_ans);
            Assert.Inconclusive("TODO: Implement code to verify target");
        }
    }
}
