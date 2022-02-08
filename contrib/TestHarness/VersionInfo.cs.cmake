using System;

namespace SummarizeTest {
    static class VersionInfo {
        public static int Show()
        {
            Console.WriteLine("Version:         1.02");

            Console.WriteLine("FDB Project Ver: " + "${FDB_VERSION}");
            Console.WriteLine("FDB Version:     " + "${FDB_VERSION_MAJOR}" + "." + "${FDB_VERSION_MINOR}");
            Console.WriteLine("Source Version:  " + "${CURRENT_GIT_VERSION}");
            return 1;
        }
    }
}