import etl.ETL;
import etl.HouseKeeping;
import etl.JobRunner;
import etl.MSG;
import etl.Master;
import etl.RCV;
import etl.ShutdownHook;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicInteger;

public class DWAuto
{
  static int lastDay = -1;
  static PrintStream rcvLog = System.out; static PrintStream masLog = System.out; static PrintStream agentLog = System.out;
  static PrintStream slaveLog = System.out; static PrintStream msgLog = System.out;

  static void createLog() {
    Calendar c = Calendar.getInstance();
    int today = c.get(5);
    String dateStr = String.format("%1$tY%1$tm%1$td", new Object[] { c });

    if (today != lastDay) {
      if (rcvLog != System.out) rcvLog.close();
      String fileName = ETL.Auto_home + "/LOG/etlrcv_" + dateStr + ".log";
      try {
        rcvLog = new PrintStream(new FileOutputStream(fileName, true));
      } catch (FileNotFoundException e) {
        e.printStackTrace(); rcvLog = System.out;
      }
      ETL.PrintVersionInfo(rcvLog, "DWAuto-rcv");

      if (masLog != System.out) masLog.close();
      fileName = ETL.Auto_home + "/LOG/etlmas_" + dateStr + ".log";
      try {
        masLog = new PrintStream(new FileOutputStream(fileName, true));
      } catch (FileNotFoundException e) {
        e.printStackTrace(); masLog = System.out;
      }
      ETL.PrintVersionInfo(masLog, "DWAuto-Master");

      if (msgLog != System.out) msgLog.close();
      fileName = ETL.Auto_home + "/LOG/etlmsg_" + dateStr + ".log";
      try {
        msgLog = new PrintStream(new FileOutputStream(fileName, true));
      } catch (FileNotFoundException e) {
        e.printStackTrace(); msgLog = System.out;
      }
      ETL.PrintVersionInfo(masLog, "DWAuto-msg");
    }
    lastDay = today;
  }

  public static void main(String[] args)
  {
    if (args.length < 1) {
      System.err.println("Please with cfgFile paramenter");
      System.exit(1);
    }
    ETL.Initialize(args[0]);

    Connection con = ETL.Connect();
    if (con == null) {
      System.err.println("Can not connect to Automation repository.!!!");
      System.exit(1);
    }
    createLog();
    int agentPort = ETL.getAgentPort(con, ETL.Auto_server);
    if (agentPort == 0) {
      System.err.println("This server is not configured in Automation repository.");
      System.exit(1);
    }
    System.out.println("AGENTPORT:" + agentPort); System.out.flush();
    boolean heartOK = ETL.HearBeat();
    if (!heartOK) {
      System.err.println("This is not first instance of DW Automation for the repository.!!!");
      System.exit(1);
    }

    new ShutdownHook();
    new JobRunner(1);
    new JobRunner(agentPort);

    RCV.CheckReceiveDir(con, rcvLog);
    Master.ResetRunningJob(con, masLog);
    while (true)
    {
      ETL.Sleep(ETL.AutoSleep);
      if (ETL.Stop_Flag) {
        System.out.println("DW Automation is stopped.");
        break;
      }
      createLog();
      if (!ETL.ping(con))
        try {
          if (con != null) con.close(); con = null;
          con = ETL.Connect(); } catch (SQLException e) {
          con = null;
        }
      if (con == null) {
        ETL.Sleep(30);
      }

      Master.doSchedule(con, masLog);
      if (ETL.serviceHold != 1) {
        Master.CheckJobQueue(con, masLog);
        RCV.CheckReceiveDir(con, rcvLog);
        Master.CheckPendingJob(con, masLog);
        MSG.CheckMessageDir(con, msgLog);
        HouseKeeping.CleanupAll(con);
      }
      ETL.HearBeat();
      System.gc();
    }
    if (JobRunner.RunningCount.get() > 0) {
      System.err.print("There are runnning jobs in Automation system!!!!\n");
      int w = 60;
      while (JobRunner.RunningCount.get() > 0) {
        ETL.Sleep(1); --w;
        if (w == 0) break;
      }
    }
    ETL.Finish_Flag = true;
    if (JobRunner.RunningCount.get() == 0) {
      System.out.println("All works has been done.");
      System.exit(0);
    } else {
      System.out.println("Maybe some job canceled!!!.");
      System.exit(0);
    }
  }
}
