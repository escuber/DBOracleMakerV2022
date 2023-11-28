using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity.Core.Common.CommandTrees;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq.Expressions;


	public class makerBase
	{
		public static void MakeExport<CT, ET>(CT fdb, ET etype) 
			where CT : DbContext
			where ET : class
		{

			var bfm = new BulkFileMaker(typeof (ET), typeof (ET));

			var tableName = //tblNme;

				typeof (ET).FullName; //+".blk";
			var filename = @"c:\" + tableName + ".blk";

			//fdb = 
				//new CT();


			using (var recout = new StreamWriter(filename))
			{
				recout.AutoFlush = true;
				foreach (var dbline in fdb.Set<ET>())
				{
					var entline = fdb.Entry<ET>(dbline);
					var lineout = bfm.convertLine(entline);
					recout.WriteLine(lineout);
					//Console.WriteLine(lineout);
					///   Console.ReadLine();
				}

				recout.Close();
			}



		}
	}
