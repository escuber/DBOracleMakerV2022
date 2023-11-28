//using ora2016.DataAccess.rbms;
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
   // using AUM.dataaccess.models;
    using rp.CommonAssemblies.dataaccess.EF;
    
    using System.Data.Entity;
    using System.Data.Entity.Infrastructure;

    public class DataBaseCopy: DataBaseCopyRepository
    {

        public static string baseDir=@"C:\dbmakers\";
        public static void Main(string[] args)
        {

            if (Directory.Exists(path)) Directory.CreateDirectory(path);
           // dbloadAll();
           //save();
//
            load();

            
        }
        private static void save()
        {

            Database.SetInitializer<rbmsDb>(null);
                //(DropCreateDatabaseAlways);

            //db =new rbmsDb();
            //new rbmsDb("rbmsDb_Out");
           
           savefiles();
        }
        private static void load()
        {
           // rbmsDb.killConections();
        Database.SetInitializer<rbmsDb>(                    null);
            //new  CreateDatabaseIfNotExists<rbmsDb>());
         //  new DropCreateDatabaseAlways<rbmsDb>());
           //new DropCreateDatabaseIfModelChanges<rbmsDb>());
            var  mdb =new rbmsDb("rbmsDb_Out");
            
            dbloadAll(mdb);

        }

     
      


        public static void savefiles()
        {
            //var fks = new List<decimal>();
            //using (var odb = new rbmsDb("rbmsDb_Out"))
            //{


            //    // get the urjfkinxes
            //    fks = odb.ACCOUNT.AsNoTracking().Where(l => !l.ACCOUNTGROUPFK.HasValue).Select(l => l.PK).Distinct().ToList();
            //}
            //var fks = new List<decimal>(sks.Count);
            //foreach (var s in sks)
            //{
            //    try
            //    {
            //        var d = decimal.Parse(s);
            //        fks.Add(d);
            //    }
            //    catch{ }


            db =new rbmsDb();
            //};
            var odb = new rbmsDb("rbmsDb_Out");

            //fks.AddRange(sks.Select(decimal.Parse).Select(b => (decimal)b));

            //List<decimal> fks = sks.Cast<decimal>().ToList();
            //GetURJ<USAGE>(i => fks.Contains(i.ACCOUNTFK),fks);


            //var xks = odb.IP_EDI_FILES_PROCESS_LOG.Where(a=>a.NOTES.IndexOf("success") < 0).Select(a=>a.EDI_FILES_PROCESS_LOG_KEY).Distinct().ToList();
            //  GetURJ<IP_EDI_FILES_PROCESS_LOG>(i => xks.Contains(i.EDI_FILES_PROCESS_LOG_KEY), xks);
            //var xks = odb.ACCOUNT.Select(a => a.CNBD1).Distinct().ToList();
            //

            //var xks = odb.EM_ACCT_PROP_LINE_DTL.Select(a => a.ACCT_PROP_LINE_DTL_KEY).Distinct().ToList();
            //GetURJ<EM_BILL_LINE_DTL>(i => xks.Contains(i.BILL_LINE_DTL_KEY), xks);
            //return;

            //db.Database.Log = s => Console.WriteLine(s);
            //db.Configuration.LazyLoadingEnabled = true;

            //var xks = odb.EM_BILL.OrderByDescending(e=>e.BILL_KEY).Skip(120000).Take(30000).Select(a => a.BILL_KEY).ToList();
            

        //var rows = db.EM_BILL_LINE2.AsNoTracking();
        GetTbl<BILLLINEDTL3_JIMG>();
            return;

            var xkss = odb.EM_ACCT_PROP_LINE_DTL.OrderByDescending(b => b.ACCT_PROP_LINE_DTL_KEY).Select(b => b.ACCT_PROP_LINE_DTL_KEY).ToList<int>();
            var xksTot = xkss.Count();
            var parter = 10;
            var xksPart = xksTot / parter;

            //var odb.EM_BILL.OrderByDescending(b => b.BILL_KEY).Select(b => b.BILL_KEY).ToList<int>();

            for (int x = 0; x <= xksPart; x++)
            {
                var keys = odb.EM_ACCT_PROP_LINE_DTL.OrderByDescending(b => b.ACCT_PROP_LINE_DTL_KEY).Skip(x * xksPart).Take(xksPart * (x + 1)).Select(b => b.ACCT_PROP_LINE_DTL_KEY).ToList<int>();
                //var part=db.EM_BILL_LINE.Select(i => keys.Contains(i.BILL_KEY));
                GetURJByPart<EM_BILL_LINE_DTL>(i => keys.Contains(i.BILL_LINE_DTL_KEY), keys, db, x);


            }



            //var xkss = odb.EM_BILL.OrderByDescending(b => b.BILL_KEY).Select(b=>b.BILL_KEY).ToList<int>();
            //var xksTot = xkss.Count();
            //var parter = 10;
            //var xksPart= xksTot / parter;

            ////var odb.EM_BILL.OrderByDescending(b => b.BILL_KEY).Select(b => b.BILL_KEY).ToList<int>();

            //for (int x = 0; x <= xksPart; x++)
            //{
            //     var keys=odb.EM_BILL.OrderByDescending(b => b.BILL_KEY).Skip(x* xksPart).Take(xksPart*(x+1)).Select(b => b.BILL_KEY).ToList<int>();
            //    //var part=db.EM_BILL_LINE.Select(i => keys.Contains(i.BILL_KEY));
            //GetURJByPart<EM_BILL_LINE>(i => keys.Contains(i.BILL_KEY), keys, db,x);


            //}



            //jglog.WriteToFile("cound"+xks.Count());
                //(b=>bill_.Select(e => e.BILL_KEY).ToList();
           /// GetURJByPart<EM_BILL_LINE>(i => xks.Contains(i.BILL_KEY),xks,db);




            ////
            //Console.WriteLine("c=" + xks.Count());
            //db = new rbmsDb();
            //Console.WriteLine("c=" + xks.Count);
            //var itms=db.EM_BILL_LINE.Where(t => xks.Contains(t.BILL_KEY));
            // Console.WriteLine("c=" + itms.Count());

            //foreach (var item in xks)
            //{
            //    jglog.wl("seaking" + item);
            //    try
            //    {
            //        Console.WriteLine("wroteone");
            //        var lst = db.EM_BILL_LINE.AsNoTracking().Where(C => C.BILL_KEY == item);

            //        foreach (var line in lst)
            //        {

            //            odb.EM_BILL_LINE.Add(line);
            //            odb.SaveChanges();
            //        }
            //    }
            //    catch (Exception ex)
            //    {
            //        Console.WriteLine(ex.ToString());
            //    }
            //}
            //          GetURJAddline(lst);
            //          jglog.WriteToFile("lasat" + lst.First().BILL_LINE_KEY);

            //      }
            //      catch(Exception ex) {
            //          jglog.WriteToFile("failigur3e" + ex.ToString()); 
            //              }

            //      };
            // // GetURJ<EM_BILL_LINE>(i => xks.Contains(i.BILL_KEY) , xks);





            // these are for acct related
            //var fks = new List<int>();
            //var odb = new rbmsDb("rbmsDb_Out");

            //var xks = odb.ACCOUNT.Distinct().ToList();

            //fks = odb.ACCOUNT.Where(l => !l.ACCOUNTGROUPFK.HasValue).Select(l => l.PK).Distinct().ToList();            
            //GetURJ<CHARGE>(i => fks.Contains(i.ACCOUNTFK.Value), fks);


            //var fks = new List<int>();
            //var odb = new rbmsDb("rbmsDb_Out");

            //var xks = odb.EM_ACCT.Where(a=>a.PROP_KEY.HasValue).Select(a=>a.PROP_KEY.Value).Distinct().ToList();            
            //GetURJ<RBMS_PROPERTY>(i => xks.Contains(i.PROP_KEY), xks);


            //var fks = new List<int>();
            //var odb = new rbmsDb("rbmsDb_Out");

            //var xks = odb.EM_BILL.Select(a => a.b).Distinct().ToList();
            // GetURJ<EM_PROVIDER_LINE>(i => odb.EM_BILL.Select(b=>b.BILL_KEY).Contains(i,bill_key), xks);


            // provider lines
            //var fks = new List<int>();
            //var odb = new rbmsDb("rbmsDb_Out");

            //  var xks = odb.EM_PROVIDER_LINE.Where(p=>p.PROVIDER_KEY.HasValue).Select(a => a.PROVIDER_KEY.Value).Distinct().ToList();
            //  //   GetURJ<EM_PROVIDER>(i => xks.Contains(i.PROVIDER_KEY), xks);
            // var xks = odb.EM_PROVIDER_LINE.Select(a => a.PROVIDER_KEY).Distinct().ToList();
            //    GetURJ<EM_PROVIDER>(i => xks.Contains(i.PROVIDER_KEY), xks);

            // getting account prop details

            //var xks = odb.RBMS_PROPERTY.Select(a => a.PROP_KEY).Distinct().ToList();
            //GetURJ<EM_ACCT_PROP>(i => xks.Contains(i.PROP_KEY), xks);

            //            var xks = odb.EM_ACCT_PROP.Select(a => a.ACCT_PROP_KEY).Distinct().ToList();
            //          GetURJ<EM_ACCT_PROP_LINE>(i => xks.Contains(i.ACCT_PROP_KEY), xks);

            //  var xks = odb.EM_ACCT_PROP_LINE.Select(a => a.ACCT_PROP_LINE_KEY).Distinct().ToList();
            //  GetURJ<EM_ACCT_PROP_LINE_DTL>(i => xks.Contains(i.ACCT_PROP_LINE_KEY), xks);


            //        Parallel.Invoke(
            //                () => {
            //                    var fks = new List<decimal>();
            //                    using (var odb = new rbmsDb("rbmsDb_Out"))
            //                    {


            //                        // get the urjfkinxes
            //                        fks = odb.ACCOUNT.AsNoTracking().Where(l => !l.ACCOUNTGROUPFK.HasValue).Select(l => l.PK).Distinct().ToList();
            //                    }
            //                    new DataBaseCopy().GetURJ<CHARGE>(i => fks.Contains(i.ACCOUNTFK), fks); },
            //                () => {
            //                    var fks = new List<decimal>();
            //                    using (var odb = new rbmsDb("rbmsDb_Out"))
            //                    {


            //                        // get the urjfkinxes
            //                        fks = odb.ACCOUNT.AsNoTracking().Where(l => !l.ACCOUNTGROUPFK.HasValue).Select(l => l.PK).Distinct().ToList();
            //                    }
            //                    new DataBaseCopy().GetURJ<METER>(i => fks.Contains(i.ACCOUNTFK), fks);

            //                }

            //);


            // GetLinesToFile
            //GetLinesToFileMisc<IP_EDI_FILES_LINE_DTL>();

            //GetEMACCT<EM_ACCT>();
            // Task.par
            //GetLinesToFileLine<IP_EDI_FILES_LINE>();
            //GetURAcct<ACCOUNT>();


            //GetLinesToFile<IP_EDI_FILES_PROCESS_LOG>();
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
                fileName + tablename + "*";// + ".blk";
            var files=Directory.GetFiles(path, filename);





            //File.Exists()

            foreach (var file in files)
            {
                BulkLoad(file, tablename, schema, db);
                //BulkLoad(db);
            }

        }
        public static void dbloadAll(DbContext db)
        {
       

            var files = Directory.GetFiles(path, "**.blk");
       
            foreach (var f in files)
            {

                var tblName = f.Replace(path, "");
                tblName = tblName.Replace(".blk", "");

                var schema = "";
                if (tblName.IndexOf("#") >= 0)
                {
                    tblName = tblName.Substring(0, tblName.IndexOf("#"));

                    schema = tblName.Substring(0, tblName.LastIndexOf("."));
                    tblName = tblName.Substring(tblName.LastIndexOf(".") + 1);
                                    }
                else
                {
                    schema = tblName.Substring(0, tblName.LastIndexOf("."));
                    tblName = tblName.Substring(tblName.LastIndexOf(".") + 1);

                }
                if ((schema.ToLower() != "urjanet")&& (schema.ToUpper() != "JGAUDETTE"))                    
                    schema = "RBMS";

                tblName = tblName.Replace(".blk", "");
                Console.WriteLine("TBL:" + schema + tblName);
              //  if (schema.ToLower() != "urjanet") 
                BulkLoad(f, tblName, schema, db);

            }



        }
        private static void dbload(string tablename, string schema, DbContext db)
        {


            var filename = path + tablename + ".blk";
            BulkLoad(filename, tablename, schema, db);

        }
        public static int BulkLoad(string fileName, string tablename, string schema, DbContext db)
        {
            //string _BulkFilePath = CCConfig.getMachVal("BulkFileShare").TrimEnd('\\');
            var fi = new System.IO.FileInfo(fileName);

            string sqlforBulbLoad = string.Format
                (
                    "BULK INSERT {3}.{0} FROM '{1}\\{2}' WITH ( FIELDTERMINATOR = '~', ROWTERMINATOR = '\\n',MAXERRORS =3000)", tablename,
                 // "c:\\dbmakers\\", "ora2016.DataAccess." + schema + "." + fi.Name, schema);
                 path, fi.Name, schema);
            //"\\10.10.100.14\\dbmakers\\", fi.Name);


            sqlforBulbLoad = sqlforBulbLoad.Replace("BULK INSERT ModelForMeters", "BULK INSERT " + schema);
            int rslt = 0;

            sqlforBulbLoad = sqlforBulbLoad.Replace(@"\\", @"\");
            jglog.WaitLog(sqlforBulbLoad);
            //Type t = MethodBase.GetCurrentMethod().DeclaringType
            //using (var db = new rb())
            //{
            db.Database.CommandTimeout = 240;
            rslt = db.Database.ExecuteSqlCommand(sqlforBulbLoad);
            //}
            return rslt;
        }


        



    }
}
