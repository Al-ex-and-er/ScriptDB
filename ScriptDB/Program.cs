using System;
using NDesk.Options;
using ScriptDBLib;

namespace ScriptDB
{
    class Program
    {
        static private bool DisplayHelp;
        static private string DatabaseName = string.Empty;
        static private string ServerName = string.Empty;
        static private string Tasks = string.Empty;
        static private string UID = string.Empty;
        static private string PWD = string.Empty;
        static private string Path = string.Empty;
        static private bool ScriptAsAlter = true;
        static private bool UseHeaders = true;
        static private string InputFile = string.Empty;

        static int Main(string[] args)
        {
            Console.WriteLine("=====================================================================");
            Console.WriteLine("=== Starting process on " + System.DateTime.Now.ToLongDateString() + " @ " + System.DateTime.Now.ToLongTimeString());
            Console.WriteLine("=====================================================================");

            var p = new OptionSet()
            {
                { "db|database=", v => DatabaseName = v },
                { "tasks=", v => Tasks = v },
                { "f|file=", v => InputFile = v },
                { "srv|server=", v => ServerName = v },
                { "uid=", v => UID = v },
                { "pwd=", v => PWD = v },
                { "path=", v => Path = v },
                { "forceCreate", v => { ScriptAsAlter &= v == null;}},
                { "noHeaders", v => { UseHeaders &= v == null;}},
                { "h|?|help",  v => DisplayHelp = v != null },
            };

            try
            {
                p.Parse(args);
            }
            catch (OptionException e)
            {
                Console.Write("Error in options: ");
                Console.WriteLine(e.Message);

                DisplayHelp = true;
            }

            if (DisplayHelp)
            {
                Console.WriteLine(@"
This application scripts database objects to a folder.
Available arguments are:
   /? : Display this message showing the available arguments.
   /server=s   : Name of the SQL server.
   /database=d : Database name to script objects.
   /uid=u      : User ID to connect to the database (if required).
   /pwd=p      : Password to connect to the database (if required).
   /path=""c:\path"" : Path to store DB objects.
   /tasks=t    : Comma separated list of tasks (SPROC, TABLE, UDF, UDT, VIEW, TRIG, PS, PF, FILE).
   /file       : input file with list of objects
   /forceCreate : script CREATE instead of ALTER
   /noHeaders   : don't add safe-style headers
Example: /Server=SERVER1\SQL2008 /Database=TestDB /Tasks=SPROC,TABLE,UDF,UDT,VIEW
Example: /Server=SERVER1\SQL2008 /Database=TestDB /File=list.txt

");
                return 1;
            }

            bool NoWay = false;

            if (string.IsNullOrEmpty(ServerName))
            {
                Console.WriteLine("Missing arguments (/SERVER)");
                NoWay = true;
            }

            if (string.IsNullOrEmpty(DatabaseName))
            {
                Console.WriteLine("Missing arguments (/DATABASE)");
                NoWay = true;
            }

            if (!string.IsNullOrEmpty(InputFile))
            {
                InputFile = System.IO.Path.GetFullPath(InputFile);
                Tasks = "File=" + InputFile;

                if (string.IsNullOrEmpty(Path))
                    Path = AppDomain.CurrentDomain.BaseDirectory;
            }

            if (string.IsNullOrEmpty(Tasks))
            {
                Console.WriteLine("Missing arguments (/TASKS)");
                NoWay = true;
            }

            if (string.IsNullOrEmpty(Path))
            {
                Console.WriteLine("Missing arguments (/PATH)");
                NoWay = true;
            }

            if (NoWay)
            {
                Console.WriteLine("Process aborted");
                return 1;
            }

            DBScripter Scripter = new DBScripter(ServerName, DatabaseName, UID, PWD, Path);
            Scripter.ProcessingObject += new DBScripter.ProcessingObjectEventHandler(objScripts_ProcessingObject);

            try
            {
                Scripter.DoTasks(Tasks, ScriptAsAlter, UseHeaders);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occured in DoTasks", 1, true);
                Console.WriteLine(ex.Message, 2, true);
                Console.WriteLine(ex.ToString(), 2, true);
                return 1;
            }
            finally
            {
                Scripter.Dispose(true);
                Scripter = null;
            }

            return 0;

        }

        static private void objScripts_ProcessingObject(string pObjectType, string pObjectName, string pState)
        {
            Console.WriteLine(string.Format("{0}:{1} {2}", pObjectType, pObjectName, pState));
        }

        static private void GetCommandLineArgs(string[] Args)
        {
        }
    }

}
