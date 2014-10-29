using ExamReport;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;
using System.Collections;

namespace test_statistic
{
    
    
    /// <summary>
    ///This is a test class for GK_databaseTest and is intended
    ///to contain all GK_databaseTest Unit Tests
    ///</summary>
    [TestClass()]
    public class GK_databaseTest
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
            excel_process answer_excel = new excel_process(@"D:\项目\给王卅的编程资料\数据库\2012高考整理完DATA\ans.xlsx");
            answer_excel.run(true);
            answer_excel.KillSpecialExcel();
            excel_process groups_excel = new excel_process(@"D:\项目\给王卅的编程资料\数据库\2012高考整理完DATA\groups.xlsx");
            groups_excel.run(false);
            groups_excel.KillSpecialExcel();
            DataTable standard_ans = answer_excel.dt; // TODO: Initialize to an appropriate value
            DataTable groups = groups_excel.dt; // TODO: Initialize to an appropriate value
            ZK_database.GroupType gtype = new ZK_database.GroupType(); // TODO: Initialize to an appropriate value
            Decimal divider = new Decimal(); // TODO: Initialize to an appropriate value
            GK_database target = new GK_database(standard_ans, groups, ZK_database.GroupType.population, 7); // TODO: Initialize to an appropriate value
            string fileadd = string.Empty; // TODO: Initialize to an appropriate value
            Form1 form = null; // TODO: Initialize to an appropriate value
            string expected = string.Empty; // TODO: Initialize to an appropriate value
            string actual;
            actual = target.DBF_data_process(@"D:\项目\给王卅的编程资料\数据库\2012高考整理完DATA\data_sxw.dbf");
            //Total_statistic statis = new Total_statistic(target._basic_data, 150m, standard_ans, 8, target._group_data, groups, 7);
            //statis.statistic_process();
            //WordCreator create = new WordCreator(statis.result);
            //create.creating_word();
            DataView dv3 = target._basic_data.DefaultView;
            dv3.RowFilter = "schoolcode IN ('01102','02102','01103','02125','02129','02126', '08159','12101','08160','05101','01117', '01118', '02105','02109','03105','02128','02106','05107','08121','08117')";
            dv3.Sort = "totalmark";
            DataTable test = dv3.ToTable();
            test.SeperateGroups(ZK_database.GroupType.population, 7);
            DataView dv3_groups = target._group_data.DefaultView;
            dv3_groups.RowFilter = "schoolcode IN ('01102','02102','01103','02125','02129','02126', '08159','12101','08160','05101','01117', '01118', '02105','02109','03105','02128','02106','05107','08121','08117')";
            dv3_groups.Sort = "totalmark";
            DataTable test_groups = dv3_groups.ToTable();
            test_groups.SeperateGroups(ZK_database.GroupType.population, 7);
            Partition_statistic statis3 = new Partition_statistic("分类整体", test, 150m, standard_ans, test_groups, groups, 7);
            statis3.statistic_process(false);
            ArrayList sdata = new ArrayList();
            DataView dv = test.DefaultView;
            dv.RowFilter = "schoolcode IN ('01102','02102','01103','02125','02129','02126', '08159','12101','08160','05101')";
            dv.Sort = "totalmark";
            DataView dv_groups = test_groups.DefaultView;
            dv_groups.RowFilter = "schoolcode IN ('01102','02102','01103','02125','02129','02126', '08159','12101','08160','05101')";
            dv_groups.Sort = "totalmark";

            Partition_statistic statis1 = new Partition_statistic("示范校一", dv.ToTable(), 150m, standard_ans, dv_groups.ToTable(), groups, 7);
            statis1.statistic_process(false);
            sdata.Add(statis1.result);
            DataView dv2 = test.DefaultView;
            dv2.RowFilter = "schoolcode IN ('01117', '01118', '02105','02109','03105','02128','02106','05107','08121','08117')";
            dv2.Sort = "totalmark";
            DataView dv2_groups = test_groups.DefaultView;
            dv2_groups.RowFilter = "schoolcode IN ('01117', '01118', '02105','02109','03105','02128','02106','05107','08121','08117')";
            dv2_groups.Sort = "totalmark";
            Partition_statistic statis2 = new Partition_statistic("示范校二", dv2.ToTable(), 150m, standard_ans, dv2_groups.ToTable(), groups, 7);
            statis2.statistic_process(false);
            sdata.Add(statis2.result);
            

            sdata.Add(statis3.result);

            //Partition_wordcreator create = new Partition_wordcreator(sdata, groups);
            //create.creating_word();
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }
    }
}
