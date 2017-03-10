package etl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

public class HouseKeeping
{
  static boolean dberr = false;
  static PrintStream log = null;
  private static int lastHour = 15;

  static void removeSubDirectory(File dirF)
  {
    if (!dirF.exists()) return;
    File[] fns = dirF.listFiles();
    if (fns != null)
      for (int i = 0; i < fns.length; ++i) fns[i].delete();
    dirF.delete();
  }
  static boolean doCleanup(Connection con) {
    int keepDays;
    int keepDays;
    try { keepDays = Integer.parseInt(ETL.cfgVar.getProperty("AUTO_KEEP_PERIOD", "30")); } catch (NumberFormatException e) {
      keepDays = 30;
    }
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(System.currentTimeMillis() - keepDays * 24 * 3600 * 1000L);
    String expiredDate = String.format("%1$tY%1$tm%1$td", new Object[] { c });

    clearupPath(ETL.Auto_home + "/DATA/fail/unknown", expiredDate);

    File logDir = new File(ETL.Auto_home + "/LOG");
    String fn;
    if (logDir.isDirectory()) {
      File[] dirList = logDir.listFiles(new ControlFilter(4));
      for (int i = 0; i < dirList.length; ++i) {
        fn = dirList[i].getName();
        String logDate = fn.substring(fn.length() - 12, fn.length() - 4);
        if (logDate.compareTo(expiredDate) < 0) {
          ETL.ShowTime(log); log.println("** Delete file:" + fn);
          dirList[i].delete();
        }
      }
    }

    ArrayList sysL = ETLSys.dbGetSys(con);
    for (ETLSys s : sysL) {
      s.cleaup(con);
    }
    return dberr;
  }

  public static boolean CleanupAll(Connection con) {
    Calendar c = Calendar.getInstance();
    int currHour = c.get(11);
    if (((lastHour + 1) % 24 == currHour) && (currHour == ETL.cleanHour)) {
      dberr = false;
      String fileName = ETL.Auto_home + "/LOG/etlclean_" + String.format("%1$tY%1$tm%1$td", new Object[] { c }) + ".log";
      try {
        log = new PrintStream(new FileOutputStream(fileName, true));
      } catch (FileNotFoundException e) {
        e.printStackTrace(); log = System.out;
      }
      ETL.ShowTime(log); log.println("Begin Cleanup ...");
      doCleanup(con);
      if (log != System.out)
        log.close();
    }
    lastHour = currHour;
    return dberr;
  }

  public static void clearupPath(String basePath, String expiredDate) {
    File baseDir = new File(basePath);
    if (!baseDir.exists()) return;
    File[] dirList = baseDir.listFiles();
    if (dirList == null) return;
    for (int i = 0; i < dirList.length; ++i)
      if (dirList[i].isDirectory())
        if (dirList[i].getName().length() >= 8)
          if (dirList[i].getName().compareTo(expiredDate) < 0) {
            log.println("Clean up the directory:" + dirList[i].getPath());
            removeSubDirectory(dirList[i]);
          }
  }
}
