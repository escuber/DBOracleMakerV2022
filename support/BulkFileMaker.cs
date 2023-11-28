using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

//namespace BulkFileMaker
//{

    public class itemInfo
    {
        public int inx { get; set; }
        public string name{ get; set; }
        public string lowerName { get; set; }
        public Type type{ get; set; }
        public string defval{ get; set; }

    }


    public class BulkFileMaker
    {

        public List<itemInfo> fromCols = null;
        public List<itemInfo> toColNames = null;
        public Type[] toColTypes = null;


        public BulkFileMaker(Type efFromType, Type EFToType)
        {

            fromCols=getPropNames(efFromType);
            toColNames = getPropNames(EFToType);
        }



        private List<itemInfo> getPropNames(Type entityType)
        {

            List<itemInfo> props = new List<itemInfo>();
            int iinx = 0;
            foreach (PropertyInfo prop in entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
               
                var fname = prop.PropertyType.AssemblyQualifiedName;
                if (fname.ToLower().IndexOf("collection") != -1) continue;
                if (fname.ToLower().IndexOf("system.") == -1) continue;
                itemInfo i = new itemInfo { name = prop.Name, lowerName = prop.Name.ToLower(), inx = iinx++, type = prop.GetType() };
                props.Add(i);
               /// Console.WriteLine(i.lowerName);
            }
            return props;
        }

        public string convertLine(DbEntityEntry dbEntry)
        {

            string[]  outline=new string[toColNames.Count()];

            

            foreach (var item in toColNames)
            {


                var fromInfo= fromCols.FirstOrDefault(c => c.lowerName == item.lowerName);
                if (fromInfo != null)
                {
                    outline[item.inx] = dbEntry.CurrentValues[fromInfo.name]+"";


                }
                
                


            }
            var ret = String.Join("~", outline);
            ret = ret.Replace("~True", "~1").Replace("~False", "~0");

            return ret;


        }

    }
//}
