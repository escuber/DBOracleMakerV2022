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

namespace rp.CommonAssemblies.dataaccess.EF
{ 
    //using AUM.dataaccess.models;
  
    //using DataAccess.rbms;
    using System.Data.Entity;
    using System.Data.Entity.Infrastructure;

    public class DataBaseCopyRepository
    {

        //public static rbmsDb outdb;//= new rbms2v2();
        public static rbmsDb db;//= new rbmsDb();
        public static string path = @"c:\dbmakers\";
        //        public static void GetURJAddline<T>(IQueryable<T> mset)
        //where T : class, new()
        //            //where T : RBMS_BILL_PER_METER, new()
        //            //public static void GetLinesToFile<RBMS_BILL_PER_METER>()

        //            //{
        //            //    var mset = db.Set<T>();
        //            //    var bfm = new BulkFileMaker(typeof(T), typeof(T));
        //            //    var tableName = typeof(T).FullName;
        //            //    var filename = path + tableName + ".blk";
        //            //    public static void GetLinesToFile<RBMS_BILL_PER_METER>()

        //        {
        //            // var odb = new rbmsDb("rbmsDb_Out");
        //          var mdb = new rbmsDb();

        //            var bfm = new BulkFileMaker(typeof(T), typeof(T));
        //            var tableName = typeof(T).FullName;
        //            var filename = path + tableName + ".blk";

        //            //if (File.Exists(filename)) File.Delete(filename);
        //            //using ()
        //            using (var recout = new StreamWriter(new FileStream(filename, FileMode.Append), Encoding.ASCII, 2048))
        //            {
        //                int lineInx = 0;
        //                recout.AutoFlush = true;
        //               // int x = 0;
        //              ///  mdb.Configuration.AutoDetectChangesEnabled = false;


        //                   var count = mset.Count();
        //                Console.WriteLine("cnt=" + count);

        //                int succinx = 0;
        //                //for(int inx=0 ;inx<count;inx++)
        //                //{
        //                //  var dbline = myset[inx];

        //                foreach (var dbline in (IQueryable<T>)mset)
        //                {
        //                    lineInx++;
        //                    try
        //                    {

        //                        var entline = mdb.Entry<T>(dbline);//
        //                                        //mdb.EM_BILL_LINE                   //db.Entry<T>((T)dbline);
        //                        var lineout = bfm.convertLine((DbEntityEntry)entline);
        //                        recout.WriteLine(lineout);

        //                        succinx++;
        //                        if (succinx % 100000 == 0)
        //                        {
        //                            Console.WriteLine(tableName + " :line=" + (succinx).ToString() + " of " + count);
        //                            GC.Collect();

        //                        }
        //                    }
        //                    catch (Exception ex)
        //                    {



        //                    }
        //                    // if (lineInx > 100) break;
        //                    //  Console.ReadLine();
        //                }
        //                Console.WriteLine("success=" + succinx + " of " + count);
        //                recout.Flush();
        //                recout.Close();
        //            }


        //        }
        public static void GetURJByPartSaver<T>(IQueryable<T> mset, int fileInx, DbContext mdb)
where T : class, new()
        {

            var bfm = new BulkFileMaker(typeof(T), typeof(T));
            var tableName = typeof(T).FullName;
            var filename = path + tableName + "_" + fileInx + ".blk";

            if (File.Exists(filename)) File.Delete(filename);
            //using ()
            using (var recout = new StreamWriter(new FileStream(filename, FileMode.OpenOrCreate), Encoding.ASCII, 2048))
            {
                int lineInx = 0;
                recout.AutoFlush = true;
                int x = 0;
                //mdb.Configuration.AutoDetectChangesEnabled = false;

                int succinx = 0;


                foreach (var dbline in (IQueryable<T>)mset)
                {
                    lineInx++;
                    try
                    {

                        var entline = mdb.Entry<T>(dbline);//
                                                           //db.Entry<T>((T)dbline);
                        var lineout = bfm.convertLine(entline);
                        recout.WriteLine(lineout);

                        succinx++;
                        if (succinx % 100000 == 0)
                        {
                            Console.WriteLine(tableName + " :line=" + (succinx).ToString() + " of ");
                            GC.Collect();

                        }
                    }
                    catch (Exception ex)
                    {



                    }
                    // if (lineInx > 100) break;
                    //  Console.ReadLine();
                }
                Console.WriteLine("success=" + succinx + " of ");
                recout.Flush();
                recout.Close();

            }

        }
        public static void GetURJByPart<T>(Expression<Func<T, bool>> predicate, List<int> fks,DbContext mdb,int inx)
    where T : class, new()            

        {
            
            

            var bfm = new BulkFileMaker(typeof(T), typeof(T));
            var tableName = typeof(T).FullName;



            //using ()
            //int inx = 0;

            var count = mdb.Set<T>().Where(predicate).Count();
            var nset =mdb.Set<T>().Where(predicate);
            GetURJByPartSaver<T>(nset, inx, mdb);



            //Console.WriteLine("count=" + count);
            //int inx = 0;
            //foreach (var nset in mdb.Set<T>().Where(predicate).Skip(0).Take(30))
            //{
            //    GetURJByPartSaver<T>(nset, inx++, mdb);


            //        try
            //        {

            //            var entline = mdb.Entry<T>(dbline);//
            //                                               //db.Entry<T>((T)dbline);
            //            var lineout = bfm.convertLine(entline);
            //            recout.WriteLine(lineout);

            //            succinx++;
            //            if (succinx % 100000 == 0)
            //            {
            //                Console.WriteLine(tableName + " :line=" + (succinx).ToString() + " of ");
            //                GC.Collect();

            //            }
            //        }
            //        catch (Exception ex)
            //        {



            //        }
            //        // if (lineInx > 100) break;
            //        //  Console.ReadLine();
            //    }
            //    Console.WriteLine("success=" + succinx + " of ");

          //  }


        }
        public static  void GetURJ<T>(Expression<Func<T, bool>> predicate, List<int> fks)
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
           // var odb = new rbmsDb("rbmsDb_Out");
            var mdb = new rbmsDb();

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
                mdb.Configuration.AutoDetectChangesEnabled = false;

                //var lineKeys = odb.ACCOUNT.Where(l => !l.ACCOUNTGROUPFK.HasValue).Select(l => l.ACCOUNTNUMBER).Distinct().ToArray();


                //db.USAGE.Where(t=>t.ACCOUNTFK)
                var mset = mdb.Set<T>().Where(predicate);// "i => fks.Contains(i.ACCOUNTFK)");
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
   //             var count = mset.Count();
///                Console.WriteLine("cnt=" + count);

                int succinx = 0;
                //for(int inx=0 ;inx<count;inx++)
                //{
                //  var dbline = myset[inx];

                foreach (var dbline in (IQueryable<T>)mset)
                {
                    lineInx++;
                    try
                    {

                        var entline = mdb.Entry<T>(dbline);//
                                                          //db.Entry<T>((T)dbline);
                        var lineout = bfm.convertLine(entline);
                        recout.WriteLine(lineout);

                        succinx++;
                        if (succinx % 100000 == 0)
                        {
                            Console.WriteLine(tableName + " :line=" + (succinx).ToString() + " of ");
                            GC.Collect();

                        }
                    }
                    catch (Exception ex)
                    {



                    }
                    // if (lineInx > 100) break;
                    //  Console.ReadLine();
                }
                Console.WriteLine("success=" + succinx + " of ");
                recout.Flush();
                recout.Close();
            }


        }
        public static void GetTbl<T>()
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
            // var odb = new rbmsDb("rbmsDb_Out");
            var mdb = new rbmsDb();

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
                mdb.Configuration.AutoDetectChangesEnabled = false;

                //var lineKeys = odb.ACCOUNT.Where(l => !l.ACCOUNTGROUPFK.HasValue).Select(l => l.ACCOUNTNUMBER).Distinct().ToArray();


                //db.USAGE.Where(t=>t.ACCOUNTFK)
                var mset = mdb.Set<T>();// "i => fks.Contains(i.ACCOUNTFK)");
                                                         //  db.Entry<T>.w
                                                         //var inxs = new List<int>();
                                                         //foreach (var d in dscs)
                                                         //{
                                                         //    var xx = db.IP_EDI_FILES_PROCESS_LOG.First(i => i.CHARGE_DESCRIPTION == d).EDI_FILES_PROCESS_LOG_KEY;
                                                         //    inxs.Add(xx);
                                                         //    //if (inxs.Count >= 20000)
                                                         //    //   break;
                                                         //}



                int succinx = 0;
                //for(int inx=0 ;inx<count;inx++)
                //{
                //  var dbline = myset[inx];

                foreach (var dbline in (IQueryable<T>)mset)
                {
                    lineInx++;
                    try
                    {

                        var entline = mdb.Entry<T>(dbline);//
                                                           //db.Entry<T>((T)dbline);
                        var lineout = bfm.convertLine(entline);
                        recout.WriteLine(lineout);

                        succinx++;
                        if (succinx % 100000 == 0)
                        {
                            Console.WriteLine(tableName + " :line=" + (succinx).ToString() + " of ");
                            GC.Collect();

                        }
                    }
                    catch (Exception ex)
                    {



                    }
                    // if (lineInx > 100) break;
                    //  Console.ReadLine();
                }
                Console.WriteLine("success=" + succinx + " of ");
                recout.Flush();
                recout.Close();
            }


        }
        public static void GetURJ<T>(Expression<Func<T, bool>> predicate, List<string> fks)
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
          //  var odb = new rbmsDb("rbmsDb_Out");
            var mdb = new rbmsDb();

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
                mdb.Configuration.AutoDetectChangesEnabled = false;

                //var lineKeys = odb.ACCOUNT.Where(l => !l.ACCOUNTGROUPFK.HasValue).Select(l => l.ACCOUNTNUMBER).Distinct().ToArray();


                //db.USAGE.Where(t=>t.ACCOUNTFK)
                var mset = mdb.Set<T>().Where(predicate);// "i => fks.Contains(i.ACCOUNTFK)");
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
                Console.WriteLine("cnt=" + count);

                int succinx = 0;
                //for(int inx=0 ;inx<count;inx++)
                //{
                //  var dbline = myset[inx];

                foreach (var dbline in (IQueryable<T>)mset)
                {
                    lineInx++;
                    try
                    {

                        var entline = mdb.Entry<T>(dbline);//
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

        public static void GetEMACCT<T>()
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

                var lineKeys = odb.ACCOUNT.Where(l=> !l.ACCOUNTGROUPFK.HasValue).Select(l => l.ACCOUNTNUMBER ).Distinct().ToArray();



                var mset = db.EM_ACCT.Where(i => lineKeys.Contains(i.ACCT_CD));
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

                foreach (var dbline in (IQueryable < T > )mset)
                {
                    lineInx++;
                    try
                    {
                        
                        var entline = db.Entry<T>(dbline);//
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
        { 
            //where T : RBMS_BILL_PER_METER, new()
            //public static void GetLinesToFile<RBMS_BILL_PER_METER>()

            //{
            //    var mset = db.Set<T>();
            //    var bfm = new BulkFileMaker(typeof(T), typeof(T));
            //    var tableName = typeof(T).FullName;
            //    var filename = path + tableName + ".blk";
            //    public static void GetLinesToFile<RBMS_BILL_PER_METER>()

        db=new rbmsDb();

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


                //var dscs= db.IP_EDI_FILES_PROCESS_LOG
                //    .Where(i => i.CHARGE_DESCRIPTION.IndexOf("Sub Account") < 0 && i.CREATED_TIME >= DateTime.Parse("07/01/21017"))4                   
                //    .Select(i=>i.EDI_FILES_PROCESS_LOG_KEY);
                //Console.WriteLine("Indexes=" + dscs.count);
                //var inxs = new List<int>();
                //foreach (var d in dscs)
                //{
                //    var xx = db.IP_EDI_FILES_PROCESS_LOG.Find(d).EDI_FILES_PROCESS_LOG_KEY;
                //    inxs.Add(xx);
                //    //if (inxs.Count() ==100)
                //      //  break;
                //}
                //DateTime d = DateTime.Parse("07/01/21017");
                var mset = db.IP_EDI_FILES_PROCESS_LOG//Where(i => inxs.Contains(i.EDI_FILES_PROCESS_LOG_KEY));
                           .Where(i =>
                              (db.IP_EDI_FILES_PROCESS_LOG
                                       .Where(ii => ii.CHARGE_DESCRIPTION.IndexOf("Sub Account") < 0 && ii.EDI_FILES_PROCESS_LOG_KEY >= 766290)
                                       .Select(ii => ii.EDI_FILES_PROCESS_LOG_KEY)
                                       ).Contains(i.EDI_FILES_PROCESS_LOG_KEY));
                
                                       
                                       
                          
                                        

                    //db.Set<T>();

                // var mset = db.RBMS_BILL_PER_METER.Where(m => m.RBMS_METER.METER_GRP_KEY == 6871).OrderBy(m => m.METER_KEY);
                //db.RBMS_BILL_PER_METER.OrderBy(m => m.METER_KEY);
                var count = mset.Count();
                Console.WriteLine("Getting :"+count);

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
                        if (succinx % 10000 == 0)
                        {
                            Console.WriteLine(tableName + " :line=" + (succinx).ToString() + " of " + count);
                            GC.Collect();

                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("exception :" +ex.ToString());


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
      
   

    }
}
