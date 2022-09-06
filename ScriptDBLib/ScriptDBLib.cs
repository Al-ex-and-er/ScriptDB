using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections;
using System.Collections.Specialized;
using System.Data;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;
using System.Collections.Generic;

namespace ScriptDBLib
{

    public class DBObj
    {
        public string DBName;
        public string Schema;
        public string ObjName;
        public string ObjType;
        public override string ToString()
        {
            if (string.IsNullOrEmpty(Schema))
                return ObjName;
            else if (string.IsNullOrEmpty(DBName))
                return Schema + '.' + ObjName;
            else
                return DBName + '.' + Schema + '.' + ObjName;
        }
    }

    public class DBScripter : IDisposable
    {
        public event ProcessingObjectEventHandler ProcessingObject;
        public delegate void ProcessingObjectEventHandler(string pObjectType, string pObjectName, string pState);

        private Database database = null;
        private Server server = null;
        private ScriptingOptions opt; //default scripting options

        private string serverName;
        private string databaseName;
        private string uID; 
        private string pWD;
        private string basePath;
        private string tasks;
        private bool scriptAsAlter; //script object as CREATE or ALTER
        private bool useHeaders; //add existance checks to the top or not

        private StringBuilder sb = new StringBuilder(500000);

        /// <summary> 
        /// 
        /// </summary> 
        /// <param name="pServerName">The name of the server to connect to</param> 
        /// <param name="pDatabaseName">The name of the database to process</param> 
        /// <param name="pUID">If required, the user ID to connect to the database</param> 
        /// <param name="pPWD">If required, the password to connect to the database</param> 
        /// <param name="pBasePath">Path where DB objects will be saved</param> 
        /// <remarks></remarks> 
        public DBScripter(string pServerName, string pDatabaseName, string pUID, string pPWD, string pBasePath)
        {
            serverName = pServerName;
            databaseName = pDatabaseName;
            uID = pUID;
            pWD = pPWD;
            basePath = pBasePath;
            tasks = "";

            //https://msdn.microsoft.com/en-us/library/microsoft.sqlserver.management.smo.scriptingoptions_properties(v=sql.120).aspx
            opt = new ScriptingOptions();
            opt.SchemaQualify = true;
            opt.AllowSystemObjects = false;
            opt.IncludeIfNotExists = false;//
            opt.NoFileGroup = false;
            opt.DriAll = true;
            opt.Default = true;
            opt.Indexes = true;
            opt.ClusteredIndexes = true;
            opt.NonClusteredIndexes = true;
            opt.ExtendedProperties = true;
            opt.Triggers = false;
            opt.Permissions = true;
            opt.NoCollation = false;
            opt.ScriptDataCompression = true;
        }

        #region " --- Methods --- "

        /// <summary> 
        /// Connects to the server and the database using integrated security or a UID and a password if provided 
        /// </summary> 
        /// <param name="pServerName">The name of the server to connect to</param> 
        /// <param name="pDatabaseName">The name of the database to process</param> 
        /// <param name="pUID">If required, the user ID to connect to the database</param> 
        /// <param name="pPWD">If required, the password to connect to the database</param> 
        /// <remarks></remarks> 
        private void Connect()
        {
            server = new Server();

            {
                server.ConnectionContext.ServerInstance = serverName;
                if (uID.Trim().Length == 0)
                {
                    server.ConnectionContext.LoginSecure = true;
                }
                else
                {
                    server.ConnectionContext.LoginSecure = false;
                    server.ConnectionContext.Login = uID;
                    server.ConnectionContext.Password = pWD;
                }
                server.ConnectionContext.Connect();
            }

            if (server == null)
            {
                throw new Exception("cDBScripts.Connect -> Error connecting to server");
            }
            else
            {
                database = server.Databases[databaseName];
                if (database == null)
                {
                    throw new Exception("cDBScripts.Connect -> Error connecting to database");
                }
            }

            server.SetDefaultInitFields(typeof(StoredProcedure), "IsSystemObject");
            server.SetDefaultInitFields(typeof(View), "IsSystemObject");
            server.SetDefaultInitFields(typeof(Table), "IsSystemObject");
            server.SetDefaultInitFields(typeof(UserDefinedFunction), "IsSystemObject");
            server.SetDefaultInitFields(typeof(Trigger), "IsSystemObject");
            server.SetDefaultInitFields(typeof(DatabaseDdlTrigger), "IsSystemObject");

        }

        /// <summary> 
        /// Branch to private methods depending on the task string passed as an argument 
        /// </summary> 
        /// <param name="pTasks">A comma separated list of tasks to do</param> 
        /// <remarks></remarks> 
        public void DoTasks(string pTasks, bool ScriptAsAlter, bool UseHeaders)
        {
        	scriptAsAlter = ScriptAsAlter;
        	useHeaders = UseHeaders;
            string InputFile = string.Empty;

            if (pTasks.Contains("File"))
            {
                InputFile = pTasks.Substring(5, pTasks.Length - 5);
                pTasks = "FILE";
            }

            if (!Directory.Exists(basePath))
                throw new Exception(String.Format("cDBScripts.Connect -> Path not found ({0})", basePath));

            tasks = pTasks;

            Connect();

            foreach (string strTask in pTasks.Split(','))
            {
                switch (strTask.Trim().ToUpper())
                {
                    case "SPROC":
                        ProcessStoredProcs();
                        break;
                    case "TABLE":
                        ProcessTables();
                        break;
                    case "UDT":
                        ProcessUDDTs();
                        break;
                    case "UDF":
                        ProcessUDFs();
                        break;
                    case "VIEW":
                        ProcessViews();
                        break;
                    case "TRIG":
                        ProcessTriggers();
                        break;
                    case "PS":
                        ProcessPartitionSchemas();
                        break;
                    case "PF":
                        ProcessPartitionFunctions();
                        break;
                    case "FILE":
                        ProcessInputFile(InputFile);
                        break;
                    default:
                        Report("---> Unknown task : " + strTask, "", "");
                        break;
                }
            }


            Report("Process completed.", "", "");

            server.ConnectionContext.Disconnect();
        }

        private void ProcessInputFile(string inputFile)
        {
            Report("Input file", "", "");
            //parse list of objects out of file
            List<string> list = new List<string>();
            using (StreamReader sr = new StreamReader(inputFile))
            {
                while (sr.Peek() >= 0)
                {
                    list.Add(sr.ReadLine().Trim());
                }
            }

            //split strings on schema and name
            List<DBObj> db_objects = new List<DBObj>();
            string db, schema, name, fullname;
            int pos1, pos2 = -1, pos3 = -1;

            foreach (string fullName in list)
            {
                fullname = fullName.Replace(".sql", "");
                pos1 = fullname.IndexOf('.');
                if (pos1 >= 0)
                    pos2 = fullname.IndexOf('.', pos1 + 1);

                if (pos2 >= 0)
                    pos3 = fullname.IndexOf('.', pos2 + 1);

                if (pos3 >= 0) //three-point name
                {
                    Console.WriteLine(String.Format("{0} is in wrong format, it should be one-point name like dbo.name or two-point name like db.dbo.name", fullname));
                    continue;
                }

                if (pos2 >= 0) //two-point name
                {
                    db = fullname.Substring(0, pos1);
                    schema = fullname.Substring(pos1 + 1, pos2 - pos1 - 1);
                    name = fullname.Substring(pos2 + 1, fullname.Length - pos2 - 1);
                }
                else if (pos1 >= 0) //one-point name
                {
                    db = databaseName;
                    schema = fullname.Substring(0, pos1);
                    name = fullname.Substring(pos1 + 1, fullname.Length - pos1 - 1);
                } else
                {
                    db = databaseName;
                    schema = "dbo";
                    name = fullname;
                }

                db_objects.Add(new DBObj() { DBName = db, Schema = schema, ObjName = name, ObjType = string.Empty });
            }

            //lookup object type
            string sql;
            object res;
            foreach(DBObj obj in db_objects)
            {
                sql = string.Format("SELECT type FROM {0}.sys.objects o join {1}.sys.schemas s on o.schema_id = s.schema_id WHERE o.name = '{2}' and s.name = '{3}'", obj.DBName, obj.DBName, obj.ObjName, obj.Schema);
                
                res = server.ConnectionContext.ExecuteScalar(sql);

                if (res == null)
                {
                    Console.WriteLine("Object {0} is not found", obj.ToString());
                    continue;
                }
                obj.ObjType = (res as string).Trim();
            }

            //script object
            string fn;
            foreach (DBObj curObj in db_objects)
            {
                if (string.IsNullOrEmpty(curObj.ObjType))
                    continue;

                switch (curObj.ObjType)
                {
                    case "P": //stored procedure
                        {
                            StoredProcedure obj = database.StoredProcedures[curObj.ObjName, curObj.Schema];

                            Report("  ", obj.Schema + "." + obj.Name, "");

                            fn = Path.Combine(basePath, obj.Schema + "." + obj.Name.Trim()) + ".sql";

                            try
                            {
                                 File.AppendAllText(fn, ScriptSP(obj));
                            }
                            catch (Exception ex)
                            {
                                Report("  ", obj.Schema + "." + obj.Name, ex.Message);
                            }

                        }
                        break;
                    case "U": //user table
                        {
                            Table obj = database.Tables[curObj.ObjName, curObj.Schema];

                            Report("  ", obj.Schema + "." + obj.Name, "");

                            fn = Path.Combine(basePath, obj.Schema + "." + obj.Name.Trim()) + ".sql";

                            try
                            {
                                File.AppendAllText(fn, ScriptTable(obj));
                            }
                            catch (Exception ex)
                            {
                                Report("  ", obj.Schema + "." + obj.Name, ex.Message);
                            }

                        }
                        break;
                }

            }

        }

        /// <summary>
        /// Calls ProcessingObject if ProcessingObject is not null
        /// </summary>
        /// <param name="pObjectType">Object type</param>
        /// <param name="pObjectName">Object name</param>
        /// <param name="pState">Object state</param>
        private void Report(string pObjectType, string pObjectName, string pState)
        {
            if (ProcessingObject != null)
                ProcessingObject(pObjectType, pObjectName, pState);
        }

        private string ScriptFunction(UserDefinedFunction obj)
        {
            sb.Length = 0;

            if (useHeaders)
            {
                sb.Append(PreFunction(obj.Name, obj.Schema, obj.FunctionType));
            }

            StringEnumerator en = obj.Script(opt).GetEnumerator();

            Regex rgx = new Regex(@"CREATE\s+FUNCTION\s", RegexOptions.IgnoreCase);
            string replacement = "ALTER FUNCTION ";

            while (en.MoveNext())
            {
            	if (scriptAsAlter)
                    sb.AppendLine(rgx.Replace(en.Current, replacement));
                else
                    sb.AppendLine(en.Current);

                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        private string ScriptView(View obj)
        {
            sb.Length = 0;

            if (useHeaders)
            {
                sb.Append(PreView(obj.Name, obj.Schema));
            }

            StringEnumerator en = obj.Script(opt).GetEnumerator();

            Regex rgx = new Regex(@"CREATE\s+VIEW\s", RegexOptions.IgnoreCase);
            string replacement = @"ALTER VIEW ";

            while (en.MoveNext())
            {
                if (scriptAsAlter)
                    sb.AppendLine(rgx.Replace(en.Current, replacement));
                else
                    sb.AppendLine(en.Current);

                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        private string ScriptTrigger(Trigger obj)
        {
            sb.Length = 0;

            if (useHeaders)
            {
                sb.Append(DropTrigger(obj.Name));
            }

            StringEnumerator en = obj.Script(opt).GetEnumerator();

            while (en.MoveNext())
            {
                sb.AppendLine(en.Current);
                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        private string ScriptDDLTrigger(DatabaseDdlTrigger obj)
        {
            sb.Length = 0;

            if (useHeaders)
            {
                sb.Append(DropTrigger(obj.Name));
            }

            StringEnumerator en = obj.Script(opt).GetEnumerator();

            while (en.MoveNext())
            {
                sb.AppendLine(en.Current);
                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        private string ScriptPS(PartitionScheme obj)
        {
            sb.Length = 0;

            if (useHeaders)
            {
                sb.Append(DropPartitionSchema(obj.Name));
            }

            StringEnumerator en = obj.Script(opt).GetEnumerator();

            while (en.MoveNext())
            {
                sb.AppendLine(en.Current);
                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        private string ScriptPF(PartitionFunction obj)
        {
            sb.Length = 0;

            if (useHeaders)
            {
                sb.Append(DropPartitionFunction(obj.Name));
            }

            StringEnumerator en = obj.Script(opt).GetEnumerator();

            while (en.MoveNext())
            {
                sb.AppendLine(en.Current);
                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        private string ScriptSP(StoredProcedure obj)
        {
            sb.Length = 0;

            if (useHeaders)
            {
                sb.Append(PreSP(obj.Name, obj.Schema));
            }

            StringEnumerator en = obj.Script(opt).GetEnumerator();

            Regex rgx = new Regex(@"CREATE\s+PROC(EDURE)?\s", RegexOptions.IgnoreCase);
            string replacement = @"ALTER PROCEDURE ";

            while (en.MoveNext())
            {
                if (scriptAsAlter)
                    sb.AppendLine(rgx.Replace(en.Current, replacement));
                else
                    sb.AppendLine(en.Current);
                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        private string ScriptObject(IScriptable obj)
        {
            StringEnumerator en = obj.Script(opt).GetEnumerator();

            sb.Length = 0;

            while (en.MoveNext())
            {
                sb.AppendLine(en.Current);
                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        private string ScriptTable(Table obj)
        {
            StringEnumerator en = obj.Script(opt).GetEnumerator();

            string[] rows = new string[500];
            int cnt = 0;
            while (en.MoveNext())
                rows[cnt++] = en.Current;

            sb.Length = 0;
            sb.AppendLine(rows[0]);
            sb.AppendLine(rows[1]);

            if (useHeaders)
            {
                sb.AppendLine(IfNotExistsTable(obj.Name, obj.Schema));
                sb.AppendLine("begin");
            }

            for (int i = 2; i < cnt; i++)
            {
                if (rows[i].Contains("CREATE TRIGGER"))
                {
                    sb.AppendLine(RunInExecuteSQL(rows[i]));
                }
                else
                    sb.AppendLine(rows[i]);
            }

            if (useHeaders)
                sb.AppendLine("end");

            return sb.ToString();
        }

        private string ScriptUDDT(UserDefinedDataType obj)
        {
            sb.Length = 0;

            //if (useHeaders)
            //{
            //    sb.Append(DropUDDT(obj.Name));
            //}

            StringEnumerator en = obj.Script(opt).GetEnumerator();

            while (en.MoveNext())
            {
                sb.AppendLine(en.Current);
                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        private string RunInExecuteSQL(string SQL)
        {
            return "exec sp_executesql '" + SQL.Replace("'", "''") + "'";
        }

        private string IfNotExistsTable(string Name, string Owner)
        {
            return String.Format(@"if not exists(select 1 from sysobjects where id = object_id('[{0}].[{1}]') and xtype = 'U')",
     Owner,
     Name);
        }

        private string PreFunction(string Name, string Owner, UserDefinedFunctionType type)
        {
            //AF = Aggregate function (CLR)
            //FS = Assembly (CLR) scalar function
            //FT = Assembly (CLR) table-valued function
            //FN = SQL scalar function
            //IF = SQL inline table-valued function
            //TF = SQL table-valued-function

            string res = "";
            switch (type)
            {
                case UserDefinedFunctionType.Inline:
                    res = String.Format(@"
if not exists(select 1 from sysobjects where id = object_id('[{0}].[{1}]') and xtype = 'IF' )
  exec sp_executesql N'CREATE FUNCTION [{0}].[{1}]() returns table as return select 1 a'
go

", Owner, Name);
                    break;
                case UserDefinedFunctionType.Scalar:
                    res = String.Format(@"
if not exists(select 1 from sysobjects where id = object_id('[{0}].[{1}]') and xtype = 'FN' )
  exec sp_executesql N'CREATE FUNCTION [{0}].[{1}]() returns int as begin return 1 end'
go

",Owner, Name);
                    break;
                case UserDefinedFunctionType.Table:
                    res = String.Format(@"
if not exists(select 1 from sysobjects where id = object_id('[{0}].[{1}]') and xtype = 'TF' )
  exec sp_executesql N'CREATE FUNCTION [{0}].[{1}]() returns @t table(i int) as begin return end'
go

", Owner, Name);
                    break;
            }

            return res;
        }

        private string PreView(string Name, string Owner)
        {
            return String.Format(@"
if not exists(select 1 from sysobjects where id = object_id('[{0}].[{1}]') and xtype = 'V' )
  exec sp_executesql N'CREATE VIEW [{0}].[{1}] as select 1 col'
go

",
     Owner,
     Name
);
        }

        private string PreSP(string Name, string Owner)
        {
            return String.Format(@"
if not exists(select 1 from sysobjects where id = object_id('[{0}].[{1}]') and xtype = 'P')
  exec sp_executesql N'CREATE PROCEDURE [{0}].[{1}] as select 1 col'
go

",
     Owner,
     Name
     );
        }

        private string DropPartitionSchema(string Name)
        {
            return String.Format(@"
if exists(select 1 from sys.partition_schemes where name = '[{0}]')
  DROP PARTITION SCHEME [{1}]
GO

", 
     Name, 
     Name);
        }

        private string DropPartitionFunction(string Name)
        {
            return String.Format(@"
if exists(select 1 from sys.partition_functions where name = '[{0}]')
  DROP PARTITION FUNCTION [{1}]
GO

", 
     Name, 
     Name);
        }
        
        private string DropTrigger(string Name)
        {
            return String.Format(@"
if exists(select 1 from sysobjects where name = '[{0}]' and xtype = 'TR')
  DROP TRIGGER [{1}]
GO

", 
     Name, 
     Name);
        }

        private void ProcessPartitionSchemas()
        {
        	Report("PartitionSchemas", "", "");

            string path = Directory.CreateDirectory(Path.Combine(basePath, "PartitionSchemas")).FullName;

            foreach (PartitionScheme obj in database.PartitionSchemes)
            {
                SaveToFile(obj, path);
            }
        }

        private void ProcessPartitionFunctions()
        {
        	Report("PartitionFunctions", "", "");

            string path = Directory.CreateDirectory(Path.Combine(basePath, "PartitionFunctions")).FullName;

            foreach (PartitionFunction obj in database.PartitionFunctions)
            {
                SaveToFile(obj, path);
            }
        }
        
        private void ProcessTriggers()
        {
            Report("Triggers", "", "");

            string path = Directory.CreateDirectory(Path.Combine(basePath, "Triggers")).FullName;

            foreach (DatabaseDdlTrigger obj in database.Triggers)
            {
                if (obj.IsSystemObject)
                    continue;

                SaveToFile(obj, path);
            }

            bool doProcessTables = tasks.Contains("TABLE");

            if (!doProcessTables) //DML triggers scripted at time of table scripting, script them if tables haven't been ordered
            foreach (Table obj in database.Tables)
            {
                if (obj.IsSystemObject)
                    continue;

                foreach (Trigger trg in obj.Triggers)
                {
                    SaveToFile(trg, path);
                }
            }

        }

        private void ProcessStoredProcs()
        {
            Report("Stored Procedures", "", "");

            string path = Directory.CreateDirectory(Path.Combine(basePath, "StoredProcedures")).FullName;

            foreach (StoredProcedure obj in database.StoredProcedures)
            {
                if (obj.IsSystemObject)
                    continue;

                SaveToFile(obj, path);
            }
        }

        private void ProcessTables()
        {
            Report("Tables", "", "");

            string path = Directory.CreateDirectory(Path.Combine(basePath, "Tables")).FullName;

            bool ProcessTriggers = tasks.Contains("TRIG");
            
            string trg_path = path;
            if (ProcessTriggers) trg_path = Directory.CreateDirectory(Path.Combine(basePath, "Triggers")).FullName;

            foreach (Table obj in database.Tables)
            {
                if (obj.IsSystemObject)
                    continue;

                SaveToFile(obj, path);

                if (ProcessTriggers) //save triggers if requested
                    foreach (Trigger trg in obj.Triggers)
                    {
                        if (trg.IsSystemObject)
                            continue;

                        SaveToFile(trg, trg_path);
                    } // foreach trigger
            } //foreach table 
        }

        private void ProcessUDFs()
        {
            Report("User Defined Functions", "", "");

            string path = Directory.CreateDirectory(Path.Combine(basePath, "Functions")).FullName;

            foreach (UserDefinedFunction obj in database.UserDefinedFunctions)
            {
                if (obj.IsSystemObject)
                    continue;

                SaveToFile(obj, path);
            }
        }

        private void ProcessUDDTs()
        {
            Report("User Defined Types", "", "");

            string path = Directory.CreateDirectory(Path.Combine(basePath, "UserTypes")).FullName;

            foreach (UserDefinedDataType obj in database.UserDefinedDataTypes)
            {
                SaveToFile(obj, path);
                //File.WriteAllText(Path.Combine(path, obj.Name.Trim()) + ".sql", ScriptObject(obj)); 
            }
        }

        private void ProcessViews()
        {
            Report("Views", "", "");

            string path = Directory.CreateDirectory(Path.Combine(basePath, "Views")).FullName;

            foreach (View obj in database.Views)
            {
                if (obj.IsSystemObject)
                    continue;

                SaveToFile(obj, path);
            }
        }

        private void SaveToFile(ScriptSchemaObjectBase obj, string path)
        {
            Report("  ", obj.Schema + "." + obj.Name, "");

            string fn = Path.Combine(path, obj.Schema + "." + obj.Name.Trim()) + ".sql";

            try
            {
                if (obj.GetType() == typeof(StoredProcedure))
                    File.WriteAllText(fn, ScriptSP(obj as StoredProcedure));
                else if (obj.GetType() == typeof(Table))
                    File.WriteAllText(fn, ScriptTable(obj as Table));
                else if (obj.GetType() == typeof(UserDefinedFunction))
                    File.WriteAllText(fn, ScriptFunction(obj as UserDefinedFunction));
                else if (obj.GetType() == typeof(View))
                    File.WriteAllText(fn, ScriptView(obj as View));
                else if (obj.GetType() == typeof(UserDefinedDataType))
                    File.WriteAllText(fn, ScriptUDDT(obj as UserDefinedDataType));
            }
            catch (Exception ex)
            {
                Report("  ", obj.Schema + "." + obj.Name, ex.Message);
                throw;
            }
        }

        private void SaveToFile(ScriptNameObjectBase obj, string path)
        {
            string ObjectName = obj.Name.Trim();

            if (obj.GetType() == typeof(Trigger))
            {
                var trg = obj as Trigger;
                ObjectName = (trg.Parent as Table).Schema + "." + ObjectName
            }

            Report("  ", ObjectName, "");

            string fn = Path.Combine(path, ObjectName) + ".sql";

            try
            {
                if (obj.GetType() == typeof(Trigger))
                    File.WriteAllText(fn, ScriptTrigger(obj as Trigger));
                if (obj.GetType() == typeof(DatabaseDdlTrigger))
                    File.WriteAllText(fn, ScriptDDLTrigger(obj as DatabaseDdlTrigger));
                if (obj.GetType() == typeof(PartitionScheme))
                    File.WriteAllText(fn, ScriptPS(obj as PartitionScheme));
                if (obj.GetType() == typeof(PartitionFunction))
                    File.WriteAllText(fn, ScriptPF(obj as PartitionFunction));
            }
            catch (Exception ex)
            {
                Report("  ", ObjectName, ex.Message);
                throw;
            }
        }

        #endregion

        #region " --- IDisposable implementation --- "

        private bool disposedValue = false;
        // To detect redundant calls 

        // IDisposable 
        public virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    // free managed resources when explicitly called 
                    database = null;
                    server = null;
                }
            }
            this.disposedValue = true;
        }


        void IDisposable.Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}