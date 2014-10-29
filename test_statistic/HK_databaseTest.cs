using ExamReport;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;

namespace test_statistic
{
    
    
    /// <summary>
    ///This is a test class for HK_databaseTest and is intended
    ///to contain all HK_databaseTest Unit Tests
    ///</summary>
    [TestClass()]
    public class HK_databaseTest
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
        ///A test for DBF_data_process
        ///</summary>
        [TestMethod()]
        public void DBF_data_processTest()
        {
            excel_process groups = new excel_process(@"D:\项目\给王卅的编程资料\测试用例\中考data\groups.xlsx");
            groups.run(false);
            groups.KillSpecialExcel();
            excel_process ans = new excel_process(@"D:\项目\给王卅的编程资料\测试用例\会考数据\ans.xlsx");
            ans.run(true);
            ans.KillSpecialExcel(); // TODO: Initialize to an appropriate value

            ExecuteMethod.HK_hierarchy hierarchy = new ExecuteMethod.HK_hierarchy();
            hierarchy.excellent_high = 100.0m;
            hierarchy.excellent_low = 85.0m;
            hierarchy.well_high = 85.0m;
            hierarchy.well_low = 70.0m;
            hierarchy.pass_high = 70.0m;
            hierarchy.pass_low = 60.0m;
            hierarchy.fail_high = 60.0m;
            hierarchy.fail_low = 0.0m;
           
            ZK_database.GroupType gtype = new ZK_database.GroupType(); // TODO: Initialize to an appropriate value
            Decimal divider = new Decimal(); // TODO: Initialize to an appropriate value
            HK_database target = new HK_database(ans.dt, groups.dt, ZK_database.GroupType.population, 7m); // TODO: Initialize to an appropriate value
            string fileadd = string.Empty; // TODO: Initialize to an appropriate value
            string expected = string.Empty; // TODO: Initialize to an appropriate value
            string actual;
            actual = target.DBF_data_process(@"D:\项目\给王卅的编程资料\测试用例\会考数据\CJ_HX.DBF");
            HK_worddata result = new HK_worddata(groups.groups_group);
            Total_statistic stat = new Total_statistic(result, target._basic_data, 100.0m, ans.dt, target._group_data, groups.dt, 7);
            stat.statistic_process(false);
            stat.HK_postprocess(hierarchy);
            WordCreator create = new WordCreator(result);
            //create.creating_HK_word("语文", "");
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }
    }
}
