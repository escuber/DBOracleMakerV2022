//using ora2016.DataAccess.rbms;
using System;
using System.Collections.Generic;
using System.Data.Entity.Core.Metadata.Edm;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;


namespace ora2016
{
    using AUM.dataaccess.models;
    using rp.CommonAssemblies.dataaccess.EF;
    //using DataAccess.rbms;
    using System.Data.Entity;
    using System.Data.Entity.Infrastructure;

    public class DataBaseCopy
    {

        public static rbmsDb outdb;//= new rbms2v2();
        public static rbmsDb db;//= new rbms2v2();
        public static string path = @"c:\dbmakers\";

        public static void Main(string[] args)
        {

            if (Directory.Exists(path)) Directory.CreateDirectory(path);

            //save();
            load();


            //Database.SetInitializer<rbmsDb>(   //         null);
            // new CreateDatabaseIfNotExists<rbmsDb>());
            //db =
            //    //new rbmsDb("");
            //new rbmsDb("rbmsDb_Out");
            ////            var myd = db.RBMS_PROPERTY.Max(p => p.UPDATE_TIME);
            //db.IP_EDI_FILES_PROCESS_LOG.Count();
            //loadfiles(db);
            //savefiles();

            
        }
        private static void save()
        {

            Database.SetInitializer<rbmsDb>(   null);

            db =new rbmsDb();
            //new rbmsDb("rbmsDb_Out");
            savefiles();
        }
        private static void load()
        {

            Database.SetInitializer<rbmsDb>(   //         null);
             new CreateDatabaseIfNotExists<rbmsDb>());
            db =
            //new rbmsDb("");
            new rbmsDb("rbmsDb_Out");
            loadfiles(db);
        }
        private static void loadfiles(DbContext db)
        {
            
            db.Database.CommandTimeout = db.Database.CommandTimeout * 3;
            var tables = new string[] { "IP_EDI_FILES_LINE_DTL","IP_EDI_FILES_LINE", "IP_EDI_FILES_PROCESS_LOG" };
            foreach (var tbl in tables)
            {
                dbload("", tbl, "rbms", db);
            }
            tables = new string[] { "ACCOUNT" };
            foreach (var tbl in tables)
            {
                dbload("", tbl, "urjanet", db);
            }
            return;
            tables = new string[] { "RBMS_BILL_PERIOD" , "RBMS_UNIT" };
            foreach (var tbl in tables)
            {
                dbload("", tbl, "rbms", db);
            }
            //"////RBMS_UNIT" };
            var t2 = new string[]{
              
"RBMS_BILL_CYCLE",
"RBMS_BILL_PERIOD",
"RBMS_BILL_PERIOD_ALT",
"RBMS_BILL_PERIOD_VIEW",
"RBMS_BILL_PER_METER",
"RBMS_COMPANY",
"RBMS_LU_BILL_CYCLE_TYPE",
"RBMS_LU_BILL_METH",
"RBMS_LU_BILL_TO_ADDR",
"RBMS_LU_CATEGORY",
"RBMS_LU_LIBRARY_TYPE",
"RBMS_LU_LINE_TYPE",
"RBMS_LU_LINE_TYPE_DESC",
"RBMS_LU_METER_CATEGORY",
"RBMS_LU_METER_ERROR",
"RBMS_LU_METER_MFG",
"RBMS_LU_METER_MODEL",
"RBMS_LU_METER_PART",
"RBMS_LU_METER_REGISTER_T",
"RBMS_LU_METER_RESET_FREQ",
"RBMS_LU_SUBM_FACT",
"RBMS_LU_UTIL_CYCLE_TYPE",
"RBMS_LU_UTIL_GRP",
"RBMS_METER",
"RBMS_METER_DATA",
"RBMS_METER_ERROR_PARTS",
"RBMS_METER_FILE",
"RBMS_METER_FILE_ERROR",
"RBMS_METER_GRP",
"RBMS_METER_PARTS",
"RBMS_METER_READ",
"RBMS_METER_TICKET",
"RBMS_METER_TIERED_RATE",
"RBMS_METER_TIERED_RATE_A",
"RBMS_MOVE_OUT_METER",
"RBMS_MOVE_OUT_METER_LINE",
"RBMS_PROPERTY",
"RBMS_PROP_BILL_PERIOD",
"RBMS_RESIDENT",
"RBMS_UNIT",
"RBMS_UNIT_ACCT",
"RBMS_UNIT_ACCT_ADJ_REQ",
"RBMS_UNIT_ACCT_BILL_PROF",
//"RBMS_UNIT_ACCT_BPROF_VIE",
"RBMS_UNIT_ACCT_RES",
"RBMS_UNIT_METER_GRP",


//            ///"RBMS_PROPERTY"
//                "RBMS_BILL_CYCLE",
//"RBMS_BILL_PER_METER",
//"RBMS_BILL_PERIOD",
/////"RBMS_BILL_PERIOD_ALT",
//"RBMS_LU_BILL_CYCLE_TYPE",
//"RBMS_LU_BILL_METH",
//"RBMS_LU_BILL_TO_ADDR",
//"RBMS_LU_CATEGORY",
//"RBMS_LU_METER_CATEGORY",
//"RBMS_LU_METER_ERROR",
//"RBMS_LU_METER_MODEL",
//"RBMS_LU_METER_PART",
//"RBMS_LU_METER_REGISTER_TYPE",
//"RBMS_LU_METER_RESET_FREQ",
//"RBMS_LU_SUBM_FACT",
//"RBMS_LU_UTIL_CYCLE_TYPE",
//"RBMS_LU_UTIL_GRP",
//"RBMS_METER",
//"RBMS_UNIT_METER_GRP",
////"RBMS_BILL_PERIOD_VIEW",
////"RBMS_LOAD_UNITS_TEMP",
////"RBMS_PROP_BILL_PERIOD",
////"RBMS_UNIT_ACCT_BPROF_VIEW",
////"RBMS_METER_FILE_ERROR",
//"RBMS_METER_GRP",
////"RBMS_METER_PARTS",
//"RBMS_METER_READ",
//"RBMS_METER_TICKET",
//"RBMS_METER_TIERED_RATE",
//"RBMS_METER_TIERED_RATE_AMOUNT",

////"RBMS_RESIDENT",

//"RBMS_UNIT_ACCT",
////"RBMS_UNIT_ACCT_ADJ_REQ",
//"RBMS_UNIT_ACCT_BILL_PROF",
//"RBMS_UNIT_ACCT_RES"


        };

            foreach (var t332 in t2)
            {
                dbload("", t332, "rbms", db);
            }



        }


        private static void savefiles()
        {

            //var outdb = new rbmsDb("rbmsDb_Out")




            // GetLinesToFile
            GetLinesToFileMisc<IP_EDI_FILES_LINE_DTL>();

           // Task.par
            //GetLinesToFileLine<IP_EDI_FILES_LINE>();
            //GetURAcct<ACCOUNT>();


          //  GetLinesToFile<ACCOUNTGROUP>();
          //GetLinesToFile<CHARGE>();
          //GetLinesToFile<METER>();
          // GetLinesToFile<URJANETVERSION>();
          /// GetLinesToFile<USAGE>();










            // GetLinesToFile<RBMS_RESIDENT>();
            // GetLinesToFile<RBMS_PROPERTY>();

            //GetLinesToFile<RBMS_BILL_CYCLE>();
            // GetLinesToFile<RBMS_BILL_PER_METER>();
            // GetLinesToFile<RBMS_BILL_PERIOD>();
            //GetLinesToFile<RBMS_BILL_PERIOD_ALT>();
            //GetLinesToFile<RBMS_LU_BILL_CYCLE_TYPE>();
            //GetLinesToFile<RBMS_LU_BILL_METH>();
            //GetLinesToFile<RBMS_LU_BILL_TO_ADDR>();
            //GetLinesToFile<RBMS_LU_CATEGORY>();
            //GetLinesToFile<RBMS_LU_METER_CATEGORY>();
            //GetLinesToFile<RBMS_LU_METER_ERROR>();


            //bad  GetLinesToFile<RBMS_LU_METER_MFG>();
            //GetLinesToFile<RBMS_METER_GRP>();
            //GetLinesToFile<RBMS_LU_METER_MODEL>();
            //GetLinesToFile<RBMS_LU_METER_PART>();
            //GetLinesToFile<RBMS_LU_METER_REGISTER_TYPE>();
            //GetLinesToFile<RBMS_LU_METER_RESET_FREQ>();
            //GetLinesToFile<RBMS_LU_SUBM_FACT>();
            //GetLinesToFile<RBMS_LU_UTIL_CYCLE_TYPE>();
            //GetLinesToFile<RBMS_LU_UTIL_GRP>();
            // GetLinesToFile<RBMS_METER>();
            // bad GetLinesToFile<RBMS_METER_DATA>();
            // skipped GetLinesToFile<RBMS_METER_ERROR_PARTS>();
            // skipped GetLinesToFile<RBMS_METER_FILE>();

            //GetLinesToFile<RBMS_UNIT_METER_GRP>();
            //GetLinesToFile<RBMS_BILL_PERIOD_VIEW>();
            //GetLinesToFile<RBMS_LOAD_UNITS_TEMP>();
            // GetLinesToFile<RBMS_PROP_BILL_PERIOD>();
            //GetLinesToFile<RBMS_UNIT_ACCT_BPROF_VIEW>();
            //GetLinesToFile<RBMS_METER_FILE_ERROR>();

            //GetLinesToFile<RBMS_METER_PARTS>();
            //GetLinesToFile<RBMS_METER_READ>();
            //   GetLinesToFile<RBMS_METER_TICKET>();
            //   GetLinesToFile<RBMS_METER_TIERED_RATE>();
            //GetLinesToFile<RBMS_METER_TIERED_RATE_AMOUNT>();


            //   //
            //    GetLinesToFile<RBMS_METER_FILE_GRP>();
            //   GetLinesToFile<RBMS_METER_FILE>();
            //            GetUALinesToFile();
            //GetLinesToFileForAppend<RBMS_UNIT_ACCT>();
            //GetLinesToFile<RBMS_UNIT_ACCT_ADJ_REQ>();
            //GetLinesToFile<RBMS_UNIT_ACCT_BILL_PROF>();
            // GetLinesToFile<RBMS_UNIT_ACCT_RES>();

        }



        public static void GetUALinesToFile()
            
            //where T : RBMS_BILL_PER_METER, new()
            //public static void GetLinesToFile<RBMS_BILL_PER_METER>()

            //{
            //    var mset = db.Set<T>();
            //    var bfm = new BulkFileMaker(typeof(T), typeof(T));
            //    var tableName = typeof(T).FullName;
            //    var filename = path + tableName + ".blk";
            //    public static void GetLinesToFile<RBMS_BILL_PER_METER>()

        {

            var bfm = new BulkFileMaker(typeof(RBMS_UNIT_ACCT), typeof(RBMS_UNIT_ACCT));
            var tableName = typeof(RBMS_UNIT_ACCT).FullName;
            var filename = path + tableName + ".blk";

            if (File.Exists(filename)) File.Delete(filename);
            //using ()
            using (var recout = new StreamWriter(new FileStream(filename, FileMode.OpenOrCreate), Encoding.Unicode, 2048))
            {
                int lineInx = 0;
                recout.AutoFlush = true;
                int x = 0;
                db.Configuration.AutoDetectChangesEnabled = false;

                var mset = db.Set<RBMS_UNIT_ACCT>().OrderByDescending(ua => ua.UPDATE_TIME).Take(1000);

                // var mset = db.RBMS_BILL_PER_METER.Where(m => m.RBMS_METER.METER_GRP_KEY == 6871).OrderBy(m => m.METER_KEY);
                //db.RBMS_BILL_PER_METER.OrderBy(m => m.METER_KEY);
                var count = mset.Count();
                int succinx = 0;
                //for(int inx=0 ;inx<count;inx++)
                //{
                //  var dbline = myset[inx];

                foreach (var dbline in mset)
                {
                    lineInx++;
                    try
                    {

                        var entline = //db.Entry<RBMS_BILL_PER_METER>( dbline);//
                                        db.Entry<RBMS_UNIT_ACCT>(dbline);
                        var lineout = bfm.convertLine(entline);
                        recout.WriteLine(lineout);

                        succinx++;
                 //       if (succinx % 100000 == 0)
                   //     {
                            Console.WriteLine(tableName + " :line=" + (succinx).ToString() + " of " + count);
                     //       GC.Collect();

                       // }
                    }
                    catch (Exception ex)
                    {



                    }
                    ///if (lineInx > 100) break;
                    ///   Console.ReadLine();
                }
                Console.WriteLine("success=" + succinx + " of " + count);
                recout.Flush();
                recout.Close();
            }


        }

        public static void GetLinesToFileMisc<T>()
          where T :class, new()
            //where T : RBMS_BILL_PER_METER, new()
            //public static void GetLinesToFile<RBMS_BILL_PER_METER>()

            //{
            //    var mset = db.Set<T>();
            //    var bfm = new BulkFileMaker(typeof(T), typeof(T));
            //    var tableName = typeof(T).FullName;
            //    var filename = path + tableName + ".blk";
            //    public static void GetLinesToFile<RBMS_BILL_PER_METER>()

        {
            var odb = new rbmsDb("rbmsDb_Out");

            var bfm = new BulkFileMaker(typeof(T), typeof(T));
            var tableName = typeof(T).FullName;
            var filename = path + tableName + ".blk";

            if (File.Exists(filename)) File.Delete(filename);
            //using ()
            using (var recout = new StreamWriter(new FileStream(filename, FileMode.OpenOrCreate), Encoding.ASCII, 2048))
            {
                int lineInx = 0;
                recout.AutoFlush = true;
                int x = 0;
                db.Configuration.AutoDetectChangesEnabled = false;

                var lineKeys = odb.IP_EDI_FILES_PROCESS_LOG.Select(l => l.EDI_FILES_LINE_KEY).Distinct().ToArray();



                var mset = db.IP_EDI_FILES_LINE_DTL.Where(i => lineKeys.Contains(i.EDI_FILES_LINE_KEY));
              //  db.Entry<T>.w
                //var inxs = new List<int>();
                //foreach (var d in dscs)
                //{
                //    var xx = db.IP_EDI_FILES_PROCESS_LOG.First(i => i.CHARGE_DESCRIPTION == d).EDI_FILES_PROCESS_LOG_KEY;
                //    inxs.Add(xx);
                //    //if (inxs.Count >= 20000)
                //    //   break;
                //}

                //var mset = db.IP_EDI_FILES_PROCESS_LOG.Where(i => inxs.Contains(i.EDI_FILES_PROCESS_LOG_KEY));


                //db.Set<T>();

                // var mset = db.RBMS_BILL_PER_METER.Where(m => m.RBMS_METER.METER_GRP_KEY == 6871).OrderBy(m => m.METER_KEY);
                //db.RBMS_BILL_PER_METER.OrderBy(m => m.METER_KEY);
                var count = mset.Count();
                int succinx = 0;
                Console.WriteLine(" count " + count);
                //for(int inx=0 ;inx<count;inx++)
                //{
                //  var dbline = myset[inx];

                foreach (IP_EDI_FILES_LINE_DTL dbline in mset)
                {
                    lineInx++;
                    try
                    {

                        var entline = db.Entry<IP_EDI_FILES_LINE_DTL>( dbline);//
                                        //db.Entry<T>((T)dbline);
                        var lineout = bfm.convertLine(entline);
                        recout.WriteLine(lineout);

                        succinx++;
                        if (succinx % 100000 == 0)
                        {
                            Console.WriteLine(tableName + " :line=" + (succinx).ToString() + " of " + count);
                            GC.Collect();

                        }
                    }
                    catch (Exception ex)
                    {



                    }
                   // if (lineInx > 100) break;
                      //  Console.ReadLine();
                }
                Console.WriteLine("success=" + succinx + " of " + count);
                recout.Flush();
                recout.Close();
            }


        }
        public static void GetLinesToFileLine<T>()
     where T : class, new()
            //where T : RBMS_BILL_PER_METER, new()
            //public static void GetLinesToFile<RBMS_BILL_PER_METER>()

            //{
            //    var mset = db.Set<T>();
            //    var bfm = new BulkFileMaker(typeof(T), typeof(T));
            //    var tableName = typeof(T).FullName;
            //    var filename = path + tableName + ".blk";
            //    public static void GetLinesToFile<RBMS_BILL_PER_METER>()

        {
            var odb = new rbmsDb("rbmsDb_Out");

            var bfm = new BulkFileMaker(typeof(T), typeof(T));
            var tableName = typeof(T).FullName;
            var filename = path + tableName + ".blk";

            if (File.Exists(filename)) File.Delete(filename);
            //using ()
            using (var recout = new StreamWriter(new FileStream(filename, FileMode.OpenOrCreate), Encoding.ASCII, 2048))
            {
                int lineInx = 0;
                recout.AutoFlush = true;
                int x = 0;
                db.Configuration.AutoDetectChangesEnabled = false;

                var lineKeys = odb.IP_EDI_FILES_PROCESS_LOG.Select(l => l.EDI_FILES_LINE_KEY).Distinct().ToArray();



                var mset = db.IP_EDI_FILES_LINE.Where(i => lineKeys.Contains(i.EDI_FILES_LINE_KEY));
                //  db.Entry<T>.w
                //var inxs = new List<int>();
                //foreach (var d in dscs)
                //{
                //    var xx = db.IP_EDI_FILES_PROCESS_LOG.First(i => i.CHARGE_DESCRIPTION == d).EDI_FILES_PROCESS_LOG_KEY;
                //    inxs.Add(xx);
                //    //if (inxs.Count >= 20000)
                //    //   break;
                //}

                //var mset = db.IP_EDI_FILES_PROCESS_LOG.Where(i => inxs.Contains(i.EDI_FILES_PROCESS_LOG_KEY));


                //db.Set<T>();

                // var mset = db.RBMS_BILL_PER_METER.Where(m => m.RBMS_METER.METER_GRP_KEY == 6871).OrderBy(m => m.METER_KEY);
                //db.RBMS_BILL_PER_METER.OrderBy(m => m.METER_KEY);
                var count = mset.Count();
                int succinx = 0;
                //for(int inx=0 ;inx<count;inx++)
                //{
                //  var dbline = myset[inx];

                foreach (IP_EDI_FILES_LINE dbline in mset)
                {
                    lineInx++;
                    try
                    {

                        var entline = db.Entry<IP_EDI_FILES_LINE>(dbline);//
                                                                              //db.Entry<T>((T)dbline);
                        var lineout = bfm.convertLine(entline);
                        recout.WriteLine(lineout);

                        succinx++;
                        if (succinx % 100000 == 0)
                        {
                            Console.WriteLine(tableName + " :line=" + (succinx).ToString() + " of " + count);
                            GC.Collect();

                        }
                    }
                    catch (Exception ex)
                    {



                    }
                   // if (lineInx > 100) break;
                    //  Console.ReadLine();
                }
                Console.WriteLine("success=" + succinx + " of " + count);
                recout.Flush();
                recout.Close();
            }


        }


        public static void GetURAcct<T>()
    where T : class, new()
            //where T : RBMS_BILL_PER_METER, new()
            //public static void GetLinesToFile<RBMS_BILL_PER_METER>()

            //{
            //    var mset = db.Set<T>();
            //    var bfm = new BulkFileMaker(typeof(T), typeof(T));
            //    var tableName = typeof(T).FullName;
            //    var filename = path + tableName + ".blk";
            //    public static void GetLinesToFile<RBMS_BILL_PER_METER>()

        {
            var odb = new rbmsDb("rbmsDb_Out");

            var bfm = new BulkFileMaker(typeof(T), typeof(T));
            var tableName = typeof(T).FullName;
            var filename = path + tableName + ".blk";

            if (File.Exists(filename)) File.Delete(filename);
            //using ()
            using (var recout = new StreamWriter(new FileStream(filename, FileMode.OpenOrCreate), Encoding.ASCII, 2048))
            {
                int lineInx = 0;
                recout.AutoFlush = true;
                int x = 0;
                db.Configuration.AutoDetectChangesEnabled = false;

                var lineKeys = odb.IP_EDI_FILES_PROCESS_LOG.Select(l => l.SVC_ACCT_NUMBER).Distinct().ToArray();



                var mset = db.ACCOUNT.Where(i => lineKeys.Contains(i.RAWACCOUNTNUMBER));
                //  db.Entry<T>.w
                //var inxs = new List<int>();
                //foreach (var d in dscs)
                //{
                //    var xx = db.IP_EDI_FILES_PROCESS_LOG.First(i => i.CHARGE_DESCRIPTION == d).EDI_FILES_PROCESS_LOG_KEY;
                //    inxs.Add(xx);
                //    //if (inxs.Count >= 20000)
                //    //   break;
                //}

                //var mset = db.IP_EDI_FILES_PROCESS_LOG.Where(i => inxs.Contains(i.EDI_FILES_PROCESS_LOG_KEY));


                //db.Set<T>();

                // var mset = db.RBMS_BILL_PER_METER.Where(m => m.RBMS_METER.METER_GRP_KEY == 6871).OrderBy(m => m.METER_KEY);
                //db.RBMS_BILL_PER_METER.OrderBy(m => m.METER_KEY);
                var count = mset.Count();
                int succinx = 0;
                //for(int inx=0 ;inx<count;inx++)
                //{
                //  var dbline = myset[inx];

                foreach (ACCOUNT dbline in mset)
                {
                    lineInx++;
                    try
                    {

                        var entline = db.Entry<ACCOUNT>(dbline);//
                                                                              //db.Entry<T>((T)dbline);
                        var lineout = bfm.convertLine(entline);
                        recout.WriteLine(lineout);

                        succinx++;
                        if (succinx % 100000 == 0)
                        {
                            Console.WriteLine(tableName + " :line=" + (succinx).ToString() + " of " + count);
                            GC.Collect();

                        }
                    }
                    catch (Exception ex)
                    {



                    }
                   // if (lineInx > 100) break;
                    //  Console.ReadLine();
                }
                Console.WriteLine("success=" + succinx + " of " + count);
                recout.Flush();
                recout.Close();
            }


        }


        public static void GetLinesToFile<T>()
            where T : class, new()
            //where T : RBMS_BILL_PER_METER, new()
            //public static void GetLinesToFile<RBMS_BILL_PER_METER>()

            //{
            //    var mset = db.Set<T>();
            //    var bfm = new BulkFileMaker(typeof(T), typeof(T));
            //    var tableName = typeof(T).FullName;
            //    var filename = path + tableName + ".blk";
            //    public static void GetLinesToFile<RBMS_BILL_PER_METER>()

        {

                var bfm = new BulkFileMaker(typeof(T), typeof(T));
                var tableName = typeof(T).FullName;
                var filename = path + tableName + ".blk";

                if (File.Exists(filename)) File.Delete(filename);
            //using ()
            using (var recout = new StreamWriter(new FileStream(filename,FileMode.OpenOrCreate), Encoding.ASCII, 2048))
            {
                int lineInx = 0;
                recout.AutoFlush = true;
                int x = 0;
                db.Configuration.AutoDetectChangesEnabled = false;


                var dscs= db.IP_EDI_FILES_PROCESS_LOG.Where(i => i.CHARGE_DESCRIPTION.IndexOf("Sub Account") < 0).Select(i=>i.CHARGE_DESCRIPTION).Distinct();
                var inxs = new List<int>();
                foreach (var d in dscs)
                {
                    var xx = db.IP_EDI_FILES_PROCESS_LOG.First(i => i.CHARGE_DESCRIPTION == d).EDI_FILES_PROCESS_LOG_KEY;
                    inxs.Add(xx);
                    //if (inxs.Count >= 20000)
                     //   break;
                }

                var mset = db.IP_EDI_FILES_PROCESS_LOG.Where(i => inxs.Contains(i.EDI_FILES_PROCESS_LOG_KEY));


                    //db.Set<T>();

                // var mset = db.RBMS_BILL_PER_METER.Where(m => m.RBMS_METER.METER_GRP_KEY == 6871).OrderBy(m => m.METER_KEY);
                //db.RBMS_BILL_PER_METER.OrderBy(m => m.METER_KEY);
                var count = mset.Count();
                int succinx = 0;
                //for(int inx=0 ;inx<count;inx++)
                //{
                //  var dbline = myset[inx];

                foreach (var dbline in mset)
                {
                    lineInx++;
                    try
                    {

                        var entline = //db.Entry<RBMS_BILL_PER_METER>( dbline);//
                                        db.Entry<IP_EDI_FILES_PROCESS_LOG>(dbline);
                        var lineout = bfm.convertLine(entline);
                        recout.WriteLine(lineout);
                        
                        succinx++;
                        if (succinx % 100000 == 0)
                        {
                            Console.WriteLine(tableName + " :line=" + (succinx).ToString() + " of " + count);
                            GC.Collect();

                        }
                    }
                    catch (Exception ex)
                    {



                    }
                    ///if (lineInx > 100) break;
                    ///   Console.ReadLine();
                }
                Console.WriteLine("success=" + succinx + " of " + count);
                recout.Flush();
                recout.Close();
            }


        }


        public static void GetLinesToFileForAppend<T>()
        where T : class, new()
            //where T : RBMS_BILL_PER_METER, new()
            //public static void GetLinesToFile<RBMS_BILL_PER_METER>()

            //{
            //    var mset = db.Set<T>();
            //    var bfm = new BulkFileMaker(typeof(T), typeof(T));
            //    var tableName = typeof(T).FullName;
            //    var filename = path + tableName + ".blk";
            //    public static void GetLinesToFile<RBMS_BILL_PER_METER>()

        {

            var bfm = new BulkFileMaker(typeof(T), typeof(T));
            var tableName = typeof(T).FullName;
            var filename = path + tableName + ".blk";
            int succinx = 0;
            if (File.Exists(filename)) File.Delete(filename);
            //using ()
            using (var recout = new StreamWriter(new FileStream(filename, FileMode.OpenOrCreate), Encoding.ASCII, 2048))
            {
                int lineInx = 0;
                recout.AutoFlush = true;
                int x = 0;
                db.Configuration.AutoDetectChangesEnabled = false;
                var mset = db.Set<T>().Take(10000);

                // var mset = db.RBMS_BILL_PER_METER.Where(m => m.RBMS_METER.METER_GRP_KEY == 6871).OrderBy(m => m.METER_KEY);
                //db.RBMS_BILL_PER_METER.OrderBy(m => m.METER_KEY);
                var count = mset.Count();
                
                //for(int inx=0 ;inx<count;inx++)
                //{
                //  var dbline = myset[inx];

                foreach (var dbline in mset)
                {
                    lineInx++;
                    try
                    {

                        var entline = //db.Entry<RBMS_BILL_PER_METER>( dbline);//
                                        db.Entry<T>(dbline);
                        var lineout = bfm.convertLine(entline);
                        recout.WriteLine(lineout);
                        Console.WriteLine(tableName + " :line=" + (x++).ToString() + " of " + count);
                        succinx++;
                        if (succinx % 10000 == 0)
                        {
                            GC.Collect();

                        }
                    }
                    catch (Exception ex)
                    {



                    }
                    ///if (lineInx > 100) break;
                    ///   Console.ReadLine();
                }
                Console.WriteLine("success=" + succinx + " of " + count);
                recout.Flush();
                recout.Close();
            }

            var tot = 40000;
            do
            {
                var newstrt = succinx;
                succinx = GetLinesToFileAppend<T>(newstrt);

            } while (succinx < tot);
        }


        public static int GetLinesToFileAppend<T>(int startKey)
        where T : class, new()
            //where T : RBMS_BILL_PER_METER, new()
            //public static void GetLinesToFile<RBMS_BILL_PER_METER>()

            //{
            //    var mset = db.Set<T>();
            //    var bfm = new BulkFileMaker(typeof(T), typeof(T));
            //    var tableName = typeof(T).FullName;
            //    var filename = path + tableName + ".blk";
            //    public static void GetLinesToFile<RBMS_BILL_PER_METER>()

        {

            var bfm = new BulkFileMaker(typeof(T), typeof(T));
            var tableName = typeof(T).FullName;
            var filename = path + tableName + ".blk";
            int succinx = 0;
            if (File.Exists(filename)) File.Delete(filename);
            //using ()
            using (var recout = new StreamWriter(new FileStream(filename, FileMode.Append), Encoding.ASCII, 2048))
            {
                int lineInx = 0;
                recout.AutoFlush = true;
                int x = 0;
                db.Configuration.AutoDetectChangesEnabled = false;
                var mset = db.Set<T>();

                // var mset = db.RBMS_BILL_PER_METER.Where(m => m.RBMS_METER.METER_GRP_KEY == 6871).OrderBy(m => m.METER_KEY);
                //db.RBMS_BILL_PER_METER.OrderBy(m => m.METER_KEY);
                var count = mset.Count();
                
                //for(int inx=0 ;inx<count;inx++)
                //{
                //  var dbline = myset[inx];

                foreach (var dbline in mset)
                {
                    succinx++;
                    if (succinx > startKey)
                    {
                       
                        lineInx++;
                        try
                        {

                            var entline = //db.Entry<RBMS_BILL_PER_METER>( dbline);//
                                            db.Entry<T>(dbline);
                            var lineout = bfm.convertLine(entline);
                            recout.WriteLine(lineout);
                            Console.WriteLine(tableName + " :line=" + (x++).ToString() + " of " + count);
                            
                            if (succinx % 10000 == 0)
                            {
                                GC.Collect();

                            }

                            if (succinx -startKey== 10000)
                            {
                                break;

                            }


                        }
                        catch (Exception ex)
                        {



                        }
                    }
                    ///if (lineInx > 100) break;
                    ///   Console.ReadLine();
                      Console.WriteLine("success=" + succinx + " of " + count);
                }

                recout.Flush();
                recout.Close();
            }
            return succinx;

        }


        public static void GetLinesSpecialToFile<T>()
            //where T : classs, new()
            where T : RBMS_BILL_PER_METER, new()
            //public static void GetLinesToFile<RBMS_BILL_PER_METER>()

            //{
            //    var mset = db.Set<T>();
            //    var bfm = new BulkFileMaker(typeof(T), typeof(T));
            //    var tableName = typeof(T).FullName;
            //    var filename = path + tableName + ".blk";
            //    public static void GetLinesToFile<RBMS_BILL_PER_METER>()

        {

            var bfm = new BulkFileMaker(typeof(RBMS_BILL_PER_METER), typeof(RBMS_BILL_PER_METER));
            var tableName = typeof(RBMS_BILL_PER_METER).FullName;
            var filename = path + tableName + ".blk";

            if (File.Exists(filename)) File.Delete(filename);

            using (var recout = new StreamWriter(filename))
            {
                int lineInx = 0;
                recout.AutoFlush = true;
                int x = 0;
                db.Configuration.AutoDetectChangesEnabled = false;
                //  var myset = db.Set<T>().OrderBy(m=>m.);

                var mset = db.RBMS_BILL_PER_METER.Where(m => m.RBMS_METER.METER_GRP_KEY == 6871).OrderBy(m => m.METER_KEY);
                //db.RBMS_BILL_PER_METER.OrderBy(m => m.METER_KEY);
                var count = mset.Count();
                int succinx = 0;
                //for(int inx=0 ;inx<count;inx++)
                //{
                //  var dbline = myset[inx];

                foreach (var dbline in mset)
                {
                    lineInx++;
                    try
                    {

                        var entline = db.Entry<RBMS_BILL_PER_METER>(dbline);// db.Entry<T>(dbline);
                        var lineout = bfm.convertLine(entline);
                        recout.WriteLine(lineout);
                        Console.WriteLine(tableName + " :line=" + (x++).ToString() + " of " + count);
                        succinx++;
                    }
                    catch (Exception ex)
                    {



                    }
                    ///if (lineInx > 100) break;
                    ///   Console.ReadLine();
                }
                Console.WriteLine("success=" + succinx + " of " + count);
                recout.Flush();
                recout.Close();
            }


        }
      
   

        public static void loadTable(string tableName, string schema, DbContext db)
        {
            dbload("" + schema + "." + tableName, tableName, schema, db);

        }

        private static void load(string fname, string tablename, string schema)
        {


            var filename = path + fname + ".blk";

            //        OberDB.BulkLoad(filename, tablename, schema);

        }
        private static void dbload(string fileName, string tablename, string schema, DbContext db)
        {


            var filename = path + "rp.CommonAssemblies.dataaccess.EF." + 
                //schema + "." +
                fileName + tablename + ".blk";
            BulkLoad(filename, tablename, schema, db);

        }
        private static void dbload(string tablename, string schema, DbContext db)
        {


            var filename = path + tablename + ".blk";
            BulkLoad(filename, tablename, schema, db);

        }
        public static int BulkLoad(string fileName, string tableName, string schema, DbContext db)
        {
            //string _BulkFilePath = CCConfig.getMachVal("BulkFileShare").TrimEnd('\\');
            var fi = new System.IO.FileInfo(fileName);

            string sqlforBulbLoad = string.Format
                (
                    "BULK INSERT {3}.{0} FROM '{1}\\{2}' WITH ( FIELDTERMINATOR = '~', ROWTERMINATOR = '\\n',MAXERRORS =3000)", tableName,
                 // "c:\\dbmakers\\", "ora2016.DataAccess." + schema + "." + fi.Name, schema);
                 path, fi.Name, schema);
            //"\\10.10.100.14\\dbmakers\\", fi.Name);


            sqlforBulbLoad = sqlforBulbLoad.Replace("BULK INSERT ModelForMeters", "BULK INSERT " + schema);
            int rslt = 0;

            sqlforBulbLoad = sqlforBulbLoad.Replace(@"\\", @"\");
            cclog.WaitLog(sqlforBulbLoad);
            //Type t = MethodBase.GetCurrentMethod().DeclaringType
            //using (var db = new rb())
            //{
            db.Database.CommandTimeout = 240;
            rslt = db.Database.ExecuteSqlCommand(sqlforBulbLoad);
            //}
            return rslt;
        }
        //private static void loadfiles()
        //{
        //    //GetLinesToFile<oberweis.dbModel.Product>();
        //    //load("oberweis.dbModel.AspNetRole","");
        //    //load("oberweis.dbModel.AspNetUsers", "");
        //    //			load("oberweis.dbModel.AspNetUserLogin", "");
        //    ///load("oberweis.dbModel.customer","oberweis.dbModel.customers", "");
        //    //load("oberweis.dbModel.Category","IPhone");
        //    //load("oberweis.dbModel.Product","IPhone");
        //    //load("oberweis.dbModel.NutritionComponent", "IPhone");
        //    //load("oberweis.dbModel.NutritionSection", "IPhone");
        //    //load("oberweis.dbModel.Price", "IPhone");
        //    //load("oberweis.dbModel.ProductNutrition", "IPhone");
        //    //load("oberweis.dbModel.order", "oberweis.dbModel.orders", "");
        //    //  load("oberweis.dbModel.orderDetail", "");



        //public static int FNextval(string P_COLUMN_NAME_C)
        //{
        //    var rtrn = DataAccessor.ExecuteCustomProcedureWithOutput("EM_SEQUENCES_PKG.NEXTVAL",
        //        new { P_COLUMN_NAME_C }, "outCResult");
        //    //cmd.AddReturnParameter(typeof(Number));
        //    //cmd.AddParameter("@P_COLUMN_NAME_C", pColumnNameC);
        //    //cmd.Execute();

        //    return rtrn;
        //}

        //}
        


        



    }
}
