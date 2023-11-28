
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace rp.CommonAssemblies.dataaccess.EF
{
    public class DBProgram
    {

        //public static rbmsDb db = new rbmsDb();
        static void Main(string[] args)
        {
            var pth = @"\\172.19.25.100\aumnet_blob_data\";

            var db = new rbmsDb();
            //Parallel.ForEach(db.AUMNET_BLOB.AsNoTracking(), r =>
            //foreach (var r in db.AUMNET_BLOB.AsNoTracking())
            //{
            //var r=db.AUMNET_BLOB.First();
            
                var r2 = db.RBMS_BLOB.First(bl => bl.BLOB_KEY == 8850575);
            var len2 = r2.BLOB_DATA.Length;
            var r = db.RBMS_BLOB.First(bl => bl.BLOB_KEY == 8711602);
            var len=r.BLOB_DATA.Length;

                var filename = @"c:\devlab\"+r.BLOB_KEY+ ".pdf";
            File.WriteAllBytes(filename, r.BLOB_DATA);
            //var dir =pth+ r.UPDATE_TIME.Year.ToString();

            //if (!Directory.Exists(dir))
            //  {
            //    Directory.CreateDirectory(dir);

            // };

            if (!File.Exists(filename))
                {
                    if(r.BLOB_DATA!=null)
                    if (r.BLOB_DATA.Length != 0)
                    {
                            File.WriteAllBytes( filename, r.BLOB_DATA);

                           // var cd = r.CREATE_TIME;

                           // File.SetCreationTime(@"\" + filename, cd);
                        }
                    //File.Delete(dir + @"\" + filename);
                    
                }
        //    });

            //File.WriteAllBytes(dir + @"\" + filename,r.BLOB_DATA);
            return;




                db.Configuration.LazyLoadingEnabled = true;
            db.Database.Log = s => Console.WriteLine(s);
            var propelm = db.RBMS_PROPERTY.FirstOrDefault(p => p.PROP_CD == "P6866");

            return;


            var b = db.RBMS_STMT_PRINT_SITE_VALID.Where(spv => spv.PERIOD_ID == "201802" && spv.RBMS_STMT_PRINT_SITE.PROP_KEY == 11882).Select(spv => spv.RBMS_STMT_PRINT_SITE.BILL_PERIOD_KEY).ToArray();
            if (b.Count() == 0)
            {
                Console.WriteLine("Error");

            }
            Console.WriteLine("Gotcha");
            return;



            //Console.WriteLine("count =" + db.ACCOUNT.Count());

            //var accts = db.ACCOUNT.Where(a => a.CNBD10 == "17630").Select(a => a.ACCOUNTNUMBER).Distinct().ToArray();//.Distinct()
            //var vals = string.Join(",",accts);
            //cclog.WriteToFile(vals);
            ///return;

            //deleteFiles(1755);

            var int_db = new rbmsDb("app_books_integration");

            //var db= new rbmsDb();

            var cnt = int_db.BOOK_PROPERTY.Count();
            Console.WriteLine("books_props="+cnt);

            cnt = db.RBMS_PROPERTY.Count();
            Console.WriteLine("props=" + cnt);
            cnt = int_db.BOOK_COMPANY.Count();
            Console.WriteLine("books comps=" + cnt);

            cnt = db.RBMS_COMPANY.Count();
            Console.WriteLine("comps=" + cnt);

            Console.ReadLine();





        }

        public static void deleteFiles(int jobKey)
        {

            rbmsDb db = new rbmsDb();

            var fileKeys = db.AUM_FILES.Where(f => f.AUM_SCHEDULE_JOB_KEY == jobKey).Select(f => f.AUM_FILES_KEY).ToArray();
            foreach (int fk in fileKeys)
            {
                deleteFileLine(fk);
                var fl = db.AUM_FILES.First(f => f.AUM_FILES_KEY== fk);
                db.AUM_FILES.Remove(fl);
                


            }
            db.SaveChanges();


        }
        
        public static void deleteFileLine(int key)
        {
            rbmsDb db = new rbmsDb();

            var filelineKeys = db.AUM_FILES_LINE.Where(f => f.AUM_FILES_KEY== key).Select(f => f.AUM_FILES_LINE_KEY).ToArray();
            foreach (var flk in filelineKeys)
            {

                deleteFileLineDetails(flk);
                var fl = db.AUM_FILES_LINE.First(f => f.AUM_FILES_LINE_KEY == flk);
                db.AUM_FILES_LINE.Remove(fl);
                db.SaveChanges();






            }



        }

        public static void deleteFileLineDetails(int fileLineKey)
        {
            rbmsDb db = new rbmsDb();
            var lines = db.AUM_FILES_LINE_DTL.AsNoTracking().Where(f => f.AUM_FILES_LINE_KEY == fileLineKey).ToArray();
            foreach (var l in lines)
            {
                if (l.AUMNET_BLOB_KEY.HasValue)
                {
                    deleteAUMBLOB(l.AUMNET_BLOB_KEY);
                }

                if (l.RBMS_BLOB_KEY.HasValue)
                {
                    deleteRBMSBlob(l.AUMNET_BLOB_KEY);
                }
                db.AUM_FILES_LINE_DTL.Remove(l);


            }
            db.SaveChanges();


        }
        public static void deleteAUMBLOB(int? key)
        {
            rbmsDb db = new rbmsDb();

            var obj = db.AUMNET_BLOB.FirstOrDefault(b => b.AUMNET_BLOB_KEY == key);

            db.AUMNET_BLOB.Remove(obj);
            db.SaveChanges();




        }
        public static void deleteRBMSBlob(int? key)
        {
            rbmsDb db = new rbmsDb();

            var obj= db.RBMS_BLOB.FirstOrDefault(b => b.BLOB_KEY == key);
            db.RBMS_BLOB.Remove(obj);
            db.SaveChanges();






        }



    }
}
