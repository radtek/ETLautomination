package etl;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class ETL
{
  static final String Version = "2.7.1_01";
  public static String Auto_home = "/ETL";
  public static String Auto_server = "ETL1";
  static String Auto_url = "jdbc:teradata://153.65.143.251/CLIENT_CHARSET=cp936";
  static String Auto_dsn = "";
  static String UserName = "wangbill";
  static String UserPass = "abcd56789";
  private static String Auto_db = "etl";
  public static String cfgInit = "";
  public static boolean Stop_Flag = false;
  public static boolean Finish_Flag = false;
  static String today = "";
  private static int event_count = 0;
  private static Hashtable<String, String> jobEvent = new Hashtable();
  public static int AutoMaxJobCount;
  public static int AutoSleep;
  public static boolean isPrimaryServer = false;
  static int cleanHour = 4;
  private static String startDateTime;
  private static AtomicInteger msgCount = new AtomicInteger();
  static Properties cfgVar;
  private static Connection lockCon = null;
  private static boolean firstCall = true;
  public static int serviceHold = 0;

  public static boolean isOKDate(String txDate)
  {
    if (txDate.length() < 8)
      return false;
    try {
      int year = Integer.parseInt(txDate.substring(0, 4));
      int month = Integer.parseInt(txDate.substring(4, 6));
      int day = Integer.parseInt(txDate.substring(6));
      if ((year < 1) || (month < 1) || (day < 1) || (month > 12) || (day > 31)) {
        return false;
      }
      if (((day < 29) && (month == 2)) || ((day < 31) && (month != 2)) || (
        (day == 31) && ((
        (month == 1) || (month == 3) || (month == 5) || (month == 7) || (month == 8) || (month == 10) || (month == 12)))))
      {
        return true;
      }

      return (month == 2) && (day == 29) && (((year % 400 == 0) || ((year % 4 == 0) && (year % 100 != 0))));
    }
    catch (NumberFormatException localNumberFormatException)
    {
    }

    return false;
  }
  public static void main(String[] args) {
    System.out.println(isOKDate("20110801"));
  }

  static boolean InsertEventLog(Connection con, String prg, String severity, String desc)
  {
    event_count = (event_count > 999) ? 0 : event_count + 1;
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(System.currentTimeMillis());
    String eventId = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS%2$s%3$03d", new Object[] { c, prg, Integer.valueOf(event_count) });
    String strCurrentTime = String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", new Object[] { c });
    String sqlText = String.format("INSERT INTO ETL_Event (EventID, EventStatus, Severity, Description, LogTime, CloseTime) VALUES ('%1$s', 'O', '%2$s', '%3$s','%4$s', null)", new Object[] { 
      eventId, severity, desc, strCurrentTime });
    try {
      Statement st = con.createStatement();
      st.execute(sqlText);
      st.close();
    } catch (SQLException ex) {
      ex.printStackTrace();
      return false;
    }
    return true;
  }
  public static void Sleep(int second) {
    try {
      for (int s = 0; s < 10 * second; ++s) {
        Thread.sleep(100L);
        if (Stop_Flag) return;
      }
    } catch (InterruptedException e) {
      Stop_Flag = true;
    }
  }

  public static Calendar FromTxDate(String txDate) {
    Calendar c = Calendar.getInstance();

    int year = Integer.parseInt(txDate.substring(0, 4));
    int month = Integer.parseInt(txDate.substring(4, 6));
    int date = Integer.parseInt(txDate.substring(6));
    c.set(year, month - 1, date);
    return c;
  }

  public static String GetToday()
  {
    Calendar c = Calendar.getInstance(); c.setTimeInMillis(System.currentTimeMillis());
    today = String.format("%1$tY%1$tm%1$td", new Object[] { c });
    return today;
  }

  public static String GetDateTime() {
    Calendar c = Calendar.getInstance(); c.setTimeInMillis(System.currentTimeMillis());
    return String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", new Object[] { c });
  }

  public static String putVarsToCommand(Properties props, String cmds) {
    int pos1 = 0;
    int pos;
    while ((pos = cmds.indexOf('$', pos1)) >= 0)
    {
      int pos;
      int pos2 = pos;
      boolean encloser = false;
      ++pos;
      if (cmds.charAt(pos) == '{') {
        encloser = true; ++pos;
      }
      int varNameLen = 0;
      while (pos + ++varNameLen < cmds.length())
        if (!cmds.substring(pos + varNameLen, pos + varNameLen + 1).matches("[a-zA-Z0-9_]")) break;
      String varName = cmds.substring(pos, pos + varNameLen);
      String val = props.getProperty(varName, "");
      pos += ((encloser) ? 1 + varNameLen : varNameLen);
      cmds = cmds.substring(0, pos2) + val + cmds.substring(pos);
      pos1 = pos2 + val.length();
    }
    return cmds;
  }

  public static void Initialize(String cfgFile)
  {
    Calendar c = Calendar.getInstance(); c.setTimeInMillis(System.currentTimeMillis());
    startDateTime = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", new Object[] { c });
    cfgInit = cfgFile;
    cfgVar = new Properties();
    try { cfgVar.load(new FileInputStream(cfgFile)); } catch (Exception e) {
      System.out.println(" !!! The initial confgiure file can not be opened. !!!");
      System.exit(1);
    }

    Auto_home = cfgVar.getProperty("AUTO_HOME", "/ETL");
    Auto_server = cfgVar.getProperty("AUTO_SERVER", "ETL1");
    Auto_url = cfgVar.getProperty("AUTO_URL", "jdbc:teradata://127.0.0.1/CLIENT_CHARSET=cp936");
    Auto_db = cfgVar.getProperty("AUTO_DB", "etl");
    Auto_dsn = cfgVar.getProperty("AUTO_DSN");
    try { AutoMaxJobCount = Integer.parseInt(cfgVar.getProperty("AUTO_JOB_COUNT", "10")); } catch (NumberFormatException e) {
      AutoMaxJobCount = 10;
    }try {
      AutoSleep = Integer.parseInt(cfgVar.getProperty("AUTO_SLEEP", "30")); } catch (NumberFormatException e) {
      AutoSleep = 30;
    }try {
      cleanHour = Integer.parseInt(cfgVar.getProperty("AUTO_CLEAN_HOUR", "4")); } catch (NumberFormatException e) {
      cleanHour = 4;
    }
    UserName = cfgVar.getProperty("UserName", "etl");
    UserPass = cfgVar.getProperty("UserPass", "etl");

    if (UserPass.length() < 16) {
      IceKey ikey = new IceKey(0);
      String sPassword = ikey.encode(UserPass, UserName);
      cfgVar.setProperty("UserPass", sPassword);
      try { cfgVar.store(new FileOutputStream(cfgFile), "DW Automation congfig file"); } catch (Exception localException1) {
      }
    } else {
      IceKey ikey = new IceKey(0);
      UserPass = ikey.decode(UserPass, UserName);
    }
    try {
      FileOutputStream errLog = new FileOutputStream(Auto_home + "/Auto_err.txt", true);
      System.setErr(new PrintStream(errLog));
    }
    catch (Exception localException2)
    {
    }
  }

  public static boolean HearBeat()
  {
    while (true)
    {
      if (lockCon == null) {
        lockCon = Connect();
        if (lockCon == null)
          return true;
      }
      try {
        if (lockCon.getAutoCommit())
          lockCon.setAutoCommit(false);
        Statement st = lockCon.createStatement();
        st.execute("LOCKING row FOR WRITE NOWAIT SELECT 1 FROM ETL_LOCKS WHERE ETL_SERVER='" + 
          Auto_server + "'");

        st.close();
      }
      catch (SQLException ex)
      {
        if (ex.getErrorCode() == 7423)
          return false;
        Sleep(30); if (Stop_Flag) return true; try
        {
          lockCon.close(); } catch (SQLException localSQLException1) {
        }lockCon = null;
      }
    }
    try {
      lockCon.commit();
      Statement st = lockCon.createStatement();
      String sql = "UPDATE ETL_LOCKS SET HeartBeat=CURRENT_TIMESTAMP(0), JobCount=" + JobRunner.RunningCount.get() + 
        "WHERE ETL_SERVER='" + Auto_server + "'";
      if (firstCall)
      {
        sql = "UPDATE ETL_LOCKS SET Start_Time=CURRENT_TIMESTAMP(0),HeartBeat=CURRENT_TIMESTAMP(0) WHERE ETL_SERVER='" + 
          Auto_server + 
          "' ELSE INSERT INTO ETL_LOCKS VALUES('" + Auto_server + "',CURRENT_TIMESTAMP(0),CURRENT_TIMESTAMP(0),0)";
      }
      st.execute(sql);
      st.close();
      firstCall = false;
    } catch (SQLException ex) {
      return true;
    }
    return true;
  }

  public static void ShowTime(PrintStream out) {
    Calendar c = Calendar.getInstance(); c.setTimeInMillis(System.currentTimeMillis());
    out.print(String.format("[%1$tH:%1$tM:%1$tS] ", new Object[] { c }));
  }

  public static boolean IsJobAlreadyHasEvent(String job, String eventDesc) {
    if ((jobEvent.containsKey(job)) && 
      (((String)jobEvent.get(job)).equals(eventDesc)))
      return true;
    jobEvent.put(job, eventDesc);
    return false;
  }

  public static void RemoveJobEventRecord(String job) {
    jobEvent.remove(job);
  }

  public static void ShowPrefixSpace(PrintStream out) {
    out.print("           ");
  }

  public static void PrintVersionInfo(PrintStream out, String servAuto) {
    out.println();
    ShowTime(out); out.println("*******************************************************************");
    ShowTime(out); out.print("* ETL Automation " + servAuto + " Program " + "2.7.1_01" + " ,Teradata 2009 Copyright. *");
    ShowTime(out); out.println("*******************************************************************");
    out.println();
  }

  public static Connection Connect() {
    Connection con = null;
    try
    {
      try {
        Class.forName("com.teradata.jdbc.TeraDriver");
      }
      catch (Throwable ex)
      {
        System.out.println("*** Error caught ***");
        ex.printStackTrace();
        System.exit(1);
      }

      con = DriverManager.getConnection(Auto_url, UserName, UserPass);
      Statement st = con.createStatement();
      ResultSet rs = st.executeQuery("select database");
      rs.next(); String currentDB = rs.getString(1);
      rs.close(); st.close();
      if (!currentDB.equalsIgnoreCase(Auto_db))
      {
        st = con.createStatement();

        st.executeUpdate("database " + Auto_db);
        st.execute("select database"); rs = st.getResultSet(); rs.next();
        rs.close(); st.close();
      }
    }
    catch (Throwable ex)
    {
      System.out.println(ex.getMessage());
      ex.printStackTrace();
      if (con != null)
        try {
          con.close();
        }
        catch (SQLException localSQLException) {
        }
      con = null;
    }
    return con;
  }

  public static boolean ping(Connection con) {
    if (con == null)
      return false;
    try
    {
      Statement st = con.createStatement();
      st.execute("select user"); ResultSet rs = st.getResultSet();
      rs.next();
      rs.close(); st.close(); } catch (SQLException e) {
      return false;
    }
    Statement st;
    return true;
  }

  public static void WriteMessageNotification(String sys, String job, String txdate, String type, String subject, String content, String logName)
  {
    PrintStream ctl_msg = null;
    int msgid = msgCount.getAndIncrement();
    String msgFileName = Auto_home + "/DATA/message/" + startDateTime + "_" + msgid + ".msg";
    try {
      ctl_msg = new PrintStream(new FileOutputStream(msgFileName));
    } catch (Exception e) {
      ShowPrefixSpace(System.err);
      System.err.println("MSG: Can not create control file:" + msgFileName);
      return;
    }
    ctl_msg.println("Automation Message Notification");
    ctl_msg.format("SYSTEM: %1$s\nJOB: %2$s\nTXDATE: %3$s\nTYPE: %4$s\n", new Object[] { sys, job, txdate, type });
    ctl_msg.format("ATT: %1$s\nSUBJECT: %2$s\nCONTENT: %3$s\n", new Object[] { logName, subject, content });
    ctl_msg.close();
  }

  public static void WriteMessageNotification(String sys, String job, String txdate, String type, String subject, String content) {
    PrintStream ctl_msg = null;
    int msgid = msgCount.getAndIncrement();
    String msgFileName = Auto_home + "/DATA/message/" + startDateTime + "_" + msgid + ".msg";
    try {
      ctl_msg = new PrintStream(new FileOutputStream(msgFileName));
    } catch (Exception e) {
      ShowPrefixSpace(System.err);
      System.err.println("MSG: Can not create control file:" + msgFileName);
      return;
    }
    ctl_msg.println("Automation Message Notification");
    ctl_msg.format("SYSTEM: %1$s\nJOB: %2$s\nTXDATE: %3$s\nTYPE: %4$s\n", new Object[] { sys, job, txdate, type });
    ctl_msg.format("ATT:\nSUBJECT: %1$s\nCONTENT: %2$s\n", new Object[] { subject, content });
    ctl_msg.close();
  }

  public static int getAgentPort(Connection con, String serverName) {
    int serverPort = 0;
    try {
      Statement s = con.createStatement();
      String sqltext = "SELECT  AgentPort, isPrimary FROM ETL_Server WHERE ETL_Server = '" + serverName + "'";
      ResultSet r = s.executeQuery(sqltext);
      if (r.next()) {
        serverPort = r.getInt(1); if (r.wasNull()) serverPort = 0;
        String yn = r.getString(2);
        isPrimaryServer = yn.equals("Y");
      }
      r.close(); s.close(); } catch (SQLException localSQLException) {
    }
    return serverPort;
  }

  public static String getLogContentText(Connection con, String logName)
  {
    String textContent = "";
    try {
      BufferedReader in = new BufferedReader(new FileReader(logName));
      while (true) {
        String line = in.readLine();
        if (line == null) break;
        textContent = textContent + "\n" + line;
      }
      in.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return textContent;
  }
}
