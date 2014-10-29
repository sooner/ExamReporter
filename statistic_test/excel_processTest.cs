using ExamReport;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace statistic_test
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
        ///A test for run
        ///</summary>
        [TestMethod()]
        public void runTest()
        {
            string filepath = string.Empty; // TODO: Initialize to an appropriate value
            bool _type = false; // TODO: Initialize to an appropriate value
            excel_process target = new excel_process(filepath, _type); // TODO: Initialize to an appropriate value
            target.run();
            Assert.Inconclusive("A method that does not return a value cannot be verified.");
        }

        /// <summary>
        ///A test for release
        ///</summary>
        [TestMethod()]
        public void releaseTest()
        {
            string filepath = string.Empty; // TODO: Initialize to an appropriate value
            bool _type = false; // TODO: Initialize to an appropriate value
            excel_process target = new excel_process(filepath, _type); // TODO: Initialize to an appropriate value
            target.release();
            Assert.Inconclusive("A method that does not return a value cannot be verified.");
        }

        /// <summary>
        ///A test for KillSpecialExcel
        ///</summary>
        [TestMethod()]
        public void KillSpecialExcelTest()
        {
            string filepath = string.Empty; // TODO: Initialize to an appropriate value
            bool _type = false; // TODO: Initialize to an appropriate value
            excel_process target = new excel_process(filepath, _type); // TODO: Initialize to an appropriate value
            target.KillSpecialExcel();
            Assert.Inconclusive("A method that does not return a value cannot be verified.");
        }

        /// <summary>
        ///A test for GetWindowThreadProcessId
        ///</summary>
        [TestMethod()]
        [DeploymentItem("ExamReport.exe")]
        public void GetWindowThreadProcessIdTest()
        {
            IntPtr hWnd = new IntPtr(); // TODO: Initialize to an appropriate value
            int lpdwProcessId = 0; // TODO: Initialize to an appropriate value
            int lpdwProcessIdExpected = 0; // TODO: Initialize to an appropriate value
            int expected = 0; // TODO: Initialize to an appropriate value
            int actual;
            actual = excel_process_Accessor.GetWindowThreadProcessId(hWnd, out lpdwProcessId);
            Assert.AreEqual(lpdwProcessIdExpected, lpdwProcessId);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for excel_process Constructor
        ///</summary>
        [TestMethod()]
        public void excel_processConstructorTest()
        {
            string filepath = string.Empty; // TODO: Initialize to an appropriate value
            bool _type = false; // TODO: Initialize to an appropriate value
            excel_process target = new excel_process(filepath, _type);
            Assert.Inconclusive("TODO: Implement code to verify target");
        }
    }
}
