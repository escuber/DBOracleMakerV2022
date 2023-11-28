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


namespace ora2016
{
    //using AUM.dataaccess.models;
    using rp.CommonAssemblies.dataaccess.EF;
    //using DataAccess.rbms;
    using System.Data.Entity;
    using System.Data.Entity.Infrastructure;

    public class test
    {

        public static string baseDir = @"C:\dbmakers\";
        public static void Main(string[] args)
        {



 

            Database.SetInitializer<rbmsDb>(                  null);
            //new createifChangedInit<rbmsDb>());

            
            var db = new rbmsDb();


            var rep = new preprocinvoiceRepository();
            var preproc=rep.getLastInvoice("55013220763");
            int c = 0;

            
            

            ///preprocinvoiceRepository.delete()
            rep.delete_accountNum("0030003208501");
            return;





            var repo = new preprocinvoiceRepository ();
            //repo.getpr

            //  4964093589
            var rslt=repo.getPreProcInvoiceIndexes("4964093589");

            WriteObjectJson.log(rslt);
            return;


            //var inv=repo.getInvoice(8057270);
            //var invs = repo.getList(x => x.BILL_PREPROC_KEY == 8037485).ToArray();
          //  var inv = invs[0];

            //var lk = inv[0].EM_BILL_PREPROC_LINE;


            //var lns=db.EM_BILL_PREPROC_LINE.Where(l => l.BILL_PREPROC_KEY == inv.BILL_PREPROC_KEY).ToArray();

            //inv.EM_BILL_PREPROC_LINE = lns;

            

            
            //var xxx = 0;

                


            //var acct = "3022001779";

            //var adata = db.ACCOUNT.OrderBy(a => a.PK).First(a => a.ACCOUNTNUMBER.ToLower().IndexOf(acct.ToLower()) >= 0);
            //var FileLineKey = adata.CNBD10;
            //var iflineKey = int.Parse(FileLineKey);

            //AUMNET_BLOB blb1 = null; ;

            //var fileLine = db.IP_EDI_FILES_LINE.First(e => e.EDI_FILES_LINE_KEY == iflineKey);
            //fileLine.FILE_STATUS = "FAILED";
            //fileLine.FILE_PROCESS_STATUS = "N";
            //var blbKey = db.AUMNET_BLOB.OrderByDescending(b => b.AUMNET_BLOB_KEY).Select(s => s.AUMNET_BLOB_KEY).First();
            ////   db.IP_EDI_FILES_LINE.Where(e=>e.)
            //fileLine.AUMNET_BLOB_KEY = blbKey;
            //var be = db.EM_BILL_ETL_SUPPORT.First(e => e.EDI_FILES_LINE_KEY == iflineKey && e.STATEMENT_ID == adata.STATEMENTID);
            //be.LU_SOURCE_ETL_STATUS = "DOWNLOADED";
            //db.SaveChanges();

            //var db2 = new rbmsDb();
        //    db2.ChangeTracker.DetectChanges();
         //   if (!db2.AUMNET_BLOB.Any(b => b.AUMNET_BLOB_KEY == fileLine.AUMNET_BLOB_KEY))
         //   {
               // var dbp = new rbmsDb("rbmsDbProd");
               // blb1 = dbp.AUMNET_BLOB.First(b => b.AUMNET_BLOB_KEY == fileLine.AUMNET_BLOB_KEY);
               // blb1.BLOB_DATA = null;

                //db.AUMNET_BLOB.Attach(blb1);

               // var b2 = new AUMNET_BLOB { BLOB_DATA = blb1.BLOB_DATA, AUMNET_BLOB_KEY = blb1.AUMNET_BLOB_KEY, CREATED_BY = "jg", UPDATED_BY = "jg", CREATE_TIME = DateTime.Now, UPDATE_TIME = DateTime.Now };
               // blb1 = db2.AUMNET_BLOB.Add(b2);
               // db2.SaveChanges();
               // fileLine.AUMNET_BLOB_KEY = iflineKey;
              //  db2.SaveChanges();


               //  var saved = db2.AUMNET_BLOB.Any(b => b.AUMNET_BLOB_KEY == fileLine.AUMNET_BLOB_KEY);
                //  db2.Entry(blb1).State = EntityState.Added;

                ///db2.SaveChanges();
           // }
            //   var blb = db.AUMNET_BLOB.First(b => b.AUMNET_BLOB_KEY == fileLine.AUMNET_BLOB_KEY);


           // var rslt = db.getFiles(FileLineKey);
           // return;
           // db.AUM_SCHEDULE_JOBS.Count();

        
           //    var cr = new createifChangedInit<rbmsDb>();
           //cr.dbloadAll();

            //var db = new rbmsDb( );
           // urjanetRepository ur =new urjanetRepository();
            //var x=ur.GetInputData("2457902325");
            //ar x = ur.buildAccountDataObject("610000041159");
            // jglog.wl(db.Database.Connection.ConnectionString.ToString());
            //var l = db.ACCOUNT.Where(t => t.ACCOUNTNUMBER == "2457902325");

            //string output = JsonConvert.SerializeObject(x, Formatting.Indented);
           // jglog.ClearLog();
           // jglog.WriteToFile(output);

            
           // return;








        //    Database.SetInitializer<rbmsDb>(                  //  null);
                                                              //new  CreateDatabaseIfNotExists<rbmsDb>());
        //new DropCreateDatabaseAlways<rbmsDb>());
            //new DropCreateDatabaseIfModelChanges<rbmsDb>());




           // var mdb =            new rbmsDb("");
           // var cnt = mdb.ACCOUNT.Count();
          //  dbloadAll(mdb);
           // var r=new createifChangedInit<rbmsDb,ACCOUNT>();

            ///r.go();

            //if (Directory.Exists(path)) Directory.CreateDirectory(path);
            // dbloadAll();
            //save();
            //

            //var db=new rbmsDb();
            //var l=db.ACCOUNT.Where(t => t.ACCOUNTNUMBER == "2457902325");
            //var k = new urjUtilController();
            //var k.
            //jglog.wl(l.Count().ToString());






        }
    }
}