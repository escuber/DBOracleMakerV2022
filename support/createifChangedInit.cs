using System;
using System.Data.Entity;
using System.IO;

public class createifChangedInit<dbc> : DropCreateDatabaseIfModelChanges<dbc>
          where dbc : DbContext, new()
//            where T :class,new()

{
    private string path = @"C:\devlab\dbmakers";

    public void go()
    {
        db = new dbc();
        InitializeDatabase(db);
        Seed(db);


    }
    //   // throw new Exception("aaa");



    //}

    private dbc db;
    protected override void Seed(dbc db)
    {

        //this.load();
        dbloadAll();
       //var cnt = db.Set<T>().CountAsync<T>().Result;

        //_Context.Database.ExecuteSqlCommand("CREATE INDEX IX_exclude ON cards(exclude)");
        //_Context.Database.ExecuteSqlCommand("CREATE INDEX IX_zrec ON cards(zrec)");
        //_Context.Database.ExecuteSqlCommand("CREATE INDEX IX_isSampleData ON cards(isSampleData)");
        //_Context.Database.ExecuteSqlCommand("CREATE INDEX IX_seqinx ON cards(CCellsJobSeqNum)");
        //_Context.Database.ExecuteSqlCommand("CREATE INDEX IX_seq ON cards(ccellsseqnum)");
        //_Context.Database.ExecuteSqlCommand("CREATE  INDEX IX_cliSeq ON cards(clientId,CCellsSeqNum)");
        //_Context.Database.ExecuteSqlCommand("CREATE INDEX IX_cardstatus ON cards(cardstatusdata)");
        //_Context.Database.ExecuteSqlCommand("CREATE INDEX IX_SFID ON cards(stogrageFileId)");



        //_Context.Database.ExecuteSqlCommand("ALTER TABLE pallets drop COLUMN  pid  ");

        //_Context.Database.ExecuteSqlCommand("ALTER TABLE pallets ADD  pid int  ");

        //_Context.Database.ExecuteSqlCommand("ALTER TABLE bundles drop COLUMN bid  ");

        //_Context.Database.ExecuteSqlCommand("ALTER TABLE bundles ADD  bid int    ");

        //_Context.Database.ExecuteSqlCommand("ALTER TABLE masters drop COLUMN mid ");

        //_Context.Database.ExecuteSqlCommand("ALTER TABLE masters ADD  mid int  ");

        //CardsDb.AddViews();
        //CardsDb.AddStoredProcs(context);



        /*
        CREATE INDEX IX_JID ON bundles(jobId)
CREATE INDEX IX_fcid ON bundles(firstcardseq)
CREATE INDEX IX_lcid ON bundles(lastcardseq)



CREATE INDEX IX_JID ON masters(jobId)
CREATE INDEX IX_fcid ON masters(firstcardseq)
CREATE INDEX IX_lcid ON masters(lastcardseq)



CREATE INDEX IX_lcid ON pallets(lastcardseq)
CREATE INDEX IX_fcid ON pallets(firstcardseq)
        */

        //_Context.SaveChanges();
        //new JobRepository().addOptions();
        //AddOptionsToDB();
        //AddZrecData();



        //CreateIndex("dbo.cardrefs", "cardStatusData", false, "cardcardstatusinx", null);
    }


    //private void load<TBL>() where TBL : class
    //{
    //    // rbmsDb.killConections();
    //    Database.SetInitializer<dbc>(                  //  null);
    //                                                   //new  CreateDatabaseIfNotExists<rbmsDb>());
    //       new DropCreateDatabaseAlways<dbc>());
    //    //new DropCreateDatabaseIfModelChanges<rbmsDb>());
    //    db = new dbc();// "rbmsDb_Out");
    //    var cnt = db.Set<TBL>().CountAsync<TBL>().Result;


    //    dbloadAll();

    //}
    public  void dbloadAll()
    {
        if (db == null) db = new dbc();

        var files = Directory.GetFiles(path, "**.blk");////"**.blk");

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
            schema = schema.Replace(@"\","");
            schema = schema.Replace(@"\", "");
            if ((schema.ToLower() != "urjanet") && (schema.ToUpper() != "JGAUDETTE"))
                schema = "RBMS";

            tblName = tblName.Replace(".blk", "");
           // Console.WriteLine("TBL:" + schema + tblName);
            //  if (schema.ToLower() != "urjanet") 
            BulkLoad(f, tblName, schema, db);

        }



    }
    private int BulkLoad(string fileName, string tablename, string schema, DbContext db)
    {
        //string _BulkFilePath = CCConfig.getMachVal("BulkFileShare").TrimEnd('\\');
        var fi = new System.IO.FileInfo(fileName);

        string sqlforBulbLoad = string.Format
            (
                "BULK INSERT {3}.{0} FROM '{1}\\{2}' WITH ( FIELDTERMINATOR = '~', ROWTERMINATOR = '\\n',MAXERRORS =3000)", tablename,
             path, fi.Name, schema);


        sqlforBulbLoad = sqlforBulbLoad.Replace("BULK INSERT ModelForMeters", "BULK INSERT " + schema);
        int rslt = 0;

        sqlforBulbLoad = sqlforBulbLoad.Replace(@"\\", @"\");
        jglog.WaitLog(sqlforBulbLoad);
        db.Database.CommandTimeout = 240;
        rslt = db.Database.ExecuteSqlCommand(sqlforBulbLoad);
        return rslt;
    }


    private void CreateIndex(string field, Type table)
    {
        // _Context.Database.ExecuteSqlCommand("CREATE INDEX IX_cardstatus ON card(cardstatusdata)");
        //String.Format("CREATE INDEX IX_{0} ON {1} ({0})", field, table.Name));
    }
    //private void load<TBL>() where TBL : class
    //{
    //    // rbmsDb.killConections();
    //    Database.SetInitializer<dbc>(                  //  null);
    //                                                      //new  CreateDatabaseIfNotExists<rbmsDb>());
    //       new DropCreateDatabaseAlways<dbc>());
    //    //new DropCreateDatabaseIfModelChanges<rbmsDb>());
    //    db=new dbc();
    //    //var cnt = mdb.ACCOUNT.Count();
    //    var cnt = db.Set<TBL>().CountAsync<TBL>().Result;


    //    dbloadAll(mdb);
    //    // loadfiles(mdb);
    //}
}


