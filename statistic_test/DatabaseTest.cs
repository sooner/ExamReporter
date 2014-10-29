using ExamReport;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;

namespace statistic_test
{
    
    
    /// <summary>
    ///This is a test class for DatabaseTest and is intended
    ///to contain all DatabaseTest Unit Tests
    ///</summary>
    [TestClass()]
    public class DatabaseTest
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
            DataTable standard_ans = null; // TODO: Initialize to an appropriate value
            DataTable groups = null; // TODO: Initialize to an appropriate value
            Database.GroupType gtype = new Database.GroupType(); // TODO: Initialize to an appropriate value
            Decimal divider = new Decimal(); // TODO: Initialize to an appropriate value
            Database target = new Database(standard_ans, groups, gtype, divider); // TODO: Initialize to an appropriate value
            string fileadd = string.Empty; // TODO: Initialize to an appropriate value
            Form1 form = null; // TODO: Initialize to an appropriate value
            string expected = string.Empty; // TODO: Initialize to an appropriate value
            string actual;
            actual = target.DBF_data_process(fileadd, form);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for Database Constructor
        ///</summary>
        [TestMethod()]
        public void DatabaseConstructorTest()
        {
            DataTable standard_ans = null; // TODO: Initialize to an appropriate value
            DataTable groups = null; // TODO: Initialize to an appropriate value
            Database.GroupType gtype = new Database.GroupType(); // TODO: Initialize to an appropriate value
            Decimal divider = new Decimal(); // TODO: Initialize to an appropriate value
            Database target = new Database(standard_ans, groups, gtype, divider);
            Assert.Inconclusive("TODO: Implement code to verify target");
        }
    }
}
