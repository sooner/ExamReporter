using ExamReport;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections;
using System.Collections.Generic;

namespace test_statistic
{
    
    
    /// <summary>
    ///This is a test class for excel_processTest and is intended
    ///to contain all excel_processTest Unit Tests
    ///</summary>
    [TestClass()]
    public class excel_processTest
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
        ///A test for getData
        ///</summary>
        [TestMethod()]
        public void getDataTest()
        {
            string filepath = string.Empty; // TODO: Initialize to an appropriate value
            excel_process target = new excel_process(@"D:\项目\给王卅的编程资料\测试用例\示范校.xlsx"); // TODO: Initialize to an appropriate value
            List<ArrayList> expected = null; // TODO: Initialize to an appropriate value
            List<ArrayList> actual;
            actual = target.getData();
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }
    }
}
