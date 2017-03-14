package etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class JobRunner extends Thread
{
  static LinkedBlockingQueue<JobRunner> jobFinishQueue = new LinkedBlockingQueue(32);
  public static AtomicInteger RunningCount = new AtomicInteger(0);
  private static Vector<JobRunner> runningJob = new Vector();
  private static String processDir;
  private static String errorDir;
  private static String completeDir;
  Vector<JobStepInfo> listStep;
  public String retLog;
  public String slaveLog;
  public Process processWork;
  public int retCode;
  String startDateTime;
  String endDateTime;
  String startDate;
  private short stepCurrent;
  private String stepCmd;
  private String workDir;
  private int AgentPort;
  Properties prop;
  static PrintStream agentLogF = System.out; static PrintStream slaveLogF = System.out;

  public JobRunner(JobRunner jobR)
  {
    this.stepCurrent = jobR.stepCurrent;
    this.startDate = jobR.startDate;
    this.slaveLog = jobR.slaveLog;
    this.prop = jobR.prop;
    this.listStep = jobR.listStep;
    JobStepInfo s = (JobStepInfo)this.listStep.elementAt(this.stepCurrent);
    this.stepCmd = s.exeCommand; this.retCode = 255;
    this.workDir = s.WorkDir;
    synchronized (runningJob) {
      runningJob.add(this);
      RunningCount.incrementAndGet();
    }
    start();
  }

  public JobRunner(Vector<JobStepInfo> list, Properties prop) {
    this.prop = prop;
    this.listStep = list;
    JobStepInfo s = (JobStepInfo)list.firstElement();
    this.stepCmd = s.exeCommand; this.retCode = 255;
    this.workDir = s.WorkDir;

    this.stepCurrent = 0;
    Calendar c = Calendar.getInstance();
    this.startDate = String.format("%1$tY%1$tm%1$td", new Object[] { c });
    this.slaveLog = "";
    synchronized (runningJob) {
      runningJob.add(this);
      RunningCount.incrementAndGet();
    }
    start();
  }

  public JobRunner(int AgentPort) {
    processDir = ETL.Auto_home + "/DATA/process";
    errorDir = ETL.Auto_home + "/DATA/fail/error";
    completeDir = ETL.Auto_home + "/DATA/complete";
    this.stepCmd = null; this.AgentPort = AgentPort;
    start();
  }

  private void removeFromList() {
    synchronized (runningJob) {
      runningJob.remove(this);
      RunningCount.decrementAndGet();
    }
  }

  public void processNextStep(Connection con)
  {
    JobStepInfo s = (JobStepInfo)this.listStep.elementAt(this.stepCurrent);
    String sys = s.sys; String job = s.job; String txDate = s.txDate;
    if (this.retCode != 0) {
      String eventDesc = "[" + sys + "," + job + "] at Step " + s.Step_No + " invoke job script " + s.ScriptName + " failed";
      ETL.InsertEventLog(con, "SLV", "H", eventDesc);
      ETL.WriteMessageNotification(sys, job, txDate, "Failed", eventDesc, "", s.logName);
    } else {
      s.updateRecordLog(con, this.retLog);
    }
    try
    {
      if ((this.stepCurrent == this.listStep.size() - 1) && 
        (this.retCode == 0))
      {
        ETL.WriteMessageNotification(sys, job, txDate, "Done", "", "", s.logName);
        trigerDownStreamJob(con);
        updateGroupChildCheckFlag(con);
        headJobDone(con);
      }

      if ((this.retCode != 0) || (this.stepCurrent == this.listStep.size() - 1)) {
        updateJobStatus(con);
        incrementJobSessionId(con);
        String ctlFile = this.prop.getProperty("CTLFILE");
        ContentControl cont = new ContentControl(processDir, ctlFile);
        if (this.retCode == 0) {
          cont.updateFileLocation(con, completeDir + "/" + sys + "/" + this.startDate, sys, job);
          cont.MoveTo(completeDir + "/" + sys + "/" + this.startDate);
        } else {
          cont.updateFileLocation(con, errorDir + "/" + sys + "/" + this.startDate, sys, job);
          cont.MoveTo(errorDir + "/" + sys + "/" + this.startDate);
        }
      }
      String headContent = "----Output for step " + s.Step_No + " ------\n";
      s.updateStepFinal(con, this.retCode, this.endDateTime, this.slaveLog + headContent + this.retLog); } catch (SQLException e) {
      e.printStackTrace();
    }
    if ((this.retCode == 0) && (this.stepCurrent < this.listStep.size() - 1)) {
      this.stepCurrent = (short)(this.stepCurrent + 1);
      ((JobStepInfo)this.listStep.elementAt(this.stepCurrent)).updateStepRunning(con, this.stepCurrent);
      new JobRunner(this);
    }
    removeFromList();
  }

  private void incrementJobSessionId(Connection con) throws SQLException {
    String sys = ((JobStepInfo)this.listStep.get(this.stepCurrent)).sys;
    String job = ((JobStepInfo)this.listStep.get(this.stepCurrent)).job;

    String sqlText = "Update ETL_job SET JobSessionID=JobSessionID+1 WHERE ETL_System='" + sys + "' AND ETL_Job='" + job + "'";
    Statement st = con.createStatement();
    st.execute(sqlText);
    st.close();
  }

  private void headJobDone(Connection con) throws SQLException {
    String sys = ((JobStepInfo)this.listStep.get(this.stepCurrent)).sys;
    String job = ((JobStepInfo)this.listStep.get(this.stepCurrent)).job;

    this.slaveLog += "Group-head Job ....\n";
    Statement st = con.createStatement();
    String sqlText = "SELECT GroupName, AutoOnChild FROM ETL_Job_Group WHERE ETL_System = '" + 
      sys + "' AND ETL_Job = '" + job + "'";
    ResultSet rs = st.executeQuery(sqlText);
    String GroupName = ""; String AutoOnChild = "N";
    if (rs.next()) { GroupName = rs.getString(1); AutoOnChild = rs.getString(2); }
    st.close();
    if (GroupName.length() == 0) {
      this.slaveLog += "This Job is not head job.\n";
      return;
    }
    this.slaveLog = (this.slaveLog + "This Job is a head job for group [" + GroupName + "]\n");

    sqlText = "UPDATE ETL_Job_GroupChild SET CheckFlag = 'N' WHERE GroupName= '" + GroupName + "'";
    st.execute(sqlText); int count = st.getUpdateCount();
    st.close();
    this.slaveLog = (this.slaveLog + "unChecked all child job for the group: [" + GroupName + "] count=" + count + "\n");

    this.slaveLog = (this.slaveLog + "Auto TurnON is '" + AutoOnChild + "'\n");
    if (AutoOnChild.charAt(0) != 'Y') {
      return;
    }
    sqlText = "UPDATE C FROM ETL_JOB C, ETL_Job_GroupChild A, \t\t(SELECT ETL_System, ETL_Job, SUM(CASE WHEN CheckFlag = 'Y' THEN 1 ELSE 0 END) chkCount  \t\tFROM ETL_Job_GroupChild \t\tWHERE  Enable = '1' AND TurnOnFlag = 'Y' \t\tGROUP BY 1,2 HAVING chkCount = 0) B \t\tSET Enable = '1' \t\tWHERE A.GroupName = '" + 
      GroupName + "' AND a.ETL_System = B.ETL_System AND A.ETL_Job= b.ETL_Job " + 
      "  AND A.ETL_System=C.ETL_System AND A.ETL_Job=C.ETL_Job";
    st.execute(sqlText); count = st.getUpdateCount();
    this.slaveLog = (this.slaveLog + sqlText + "\n");
    this.slaveLog = (this.slaveLog + "Turn-ON all child job for the group: [" + GroupName + "] count=" + count + "\n");
    st.close();
  }

  private void updateGroupChildCheckFlag(Connection con) throws SQLException {
    String sys = ((JobStepInfo)this.listStep.get(this.stepCurrent)).sys;
    String job = ((JobStepInfo)this.listStep.get(this.stepCurrent)).job;
    String txDate = ((JobStepInfo)this.listStep.get(this.stepCurrent)).txDate;

    String sqlText = "UPDATE ETL_Job_GroupChild SET CheckFlag = 'Y', TxDate = CAST('" + 
      txDate + "' AS DATE FORMAT 'YYYYMMDD') " + 
      "   WHERE ETL_System = '" + sys + "' AND ETL_Job = '" + job + "'";
    sqlText = sqlText + ";INSERT INTO ETL_Job_Queue(ETL_System, ETL_Job,ETL_Server,TXDate,RequestTime, SeqID) SELECT B.ETL_System, B.ETL_Job, ETL_Server, CAST('" + 
      txDate + "' AS DATE FORMAT 'YYYYMMDD'), '" + 
      ETL.GetDateTime() + "', 0 " + 
      "FROM ETL_Job_Group A, ETL_Job B " + 
      " WHERE " + 
      " A.ETL_System = B.ETL_System AND A.ETL_Job = B.ETL_Job AND " + 
      " A.GroupName IN ( " + 
      " SELECT GroupName FROM ETL_Job_GroupChild A " + 
      " WHERE  GroupName NOT IN(SELECT GroupName FROM ETL_Job_GroupChild " + 
      " WHERE (CheckFlag IS NULL OR CheckFlag ='N') AND Enable = '1') " + 
      " AND ETL_System='" + sys + "' AND ETL_Job='" + job + "' AND Enable = '1') ";
    this.slaveLog += "Group-child Job  ....\n";
    Statement st = con.createStatement();
    st.execute(sqlText); int count = st.getUpdateCount();
    if (count > 0) {
      this.slaveLog = (this.slaveLog + "The Job[" + sys + "," + job + "] is belonged " + count + " group.\n");
    }
    st.getMoreResults(); count = st.getUpdateCount();
    st.close();
    if (count > 0)
      this.slaveLog = (this.slaveLog + count + " Head Job has been trigered.\n");
  }

  private void updateJobStatus(Connection con) throws SQLException {
    String sys = ((JobStepInfo)this.listStep.get(this.stepCurrent)).sys;
    String job = ((JobStepInfo)this.listStep.get(this.stepCurrent)).job;
    String setCols = "Last_JobStatus = " + ((this.retCode == 0) ? "'Done'" : "'Failed'") + 
      ", Last_EndTime='" + this.endDateTime + "', CheckFlag=" + ((this.retCode == 0) ? "'Y'" : "'N'");
    if (this.retCode == 0) {
      setCols = setCols + ", Enable= CASE WHEN AutoOff='Y' THEN '0' ELSE '1' END";
    }
    String sqlText = "Update ETL_job SET " + setCols + " WHERE ETL_System='" + sys + "' AND ETL_Job='" + job + "'";
    sqlText = sqlText + ";INSERT INTO ETL_Job_Status(ETL_System, ETL_Job, JobSessionID, TXDate, StartTime, EndTime, JobStatus, FileCnt, CubeStatus, ExpectedRecord) SELECT ETL_System, ETL_Job, JobSessionID, Last_TXDate, Last_StartTime, Last_EndTime, Last_JobStatus, Last_FileCnt,    Last_CubeStatus, ExpectedRecord FROM ETL_Job WHERE ETL_System ='" + 
      sys + "' AND ETL_Job = '" + job + "'";
    Statement st = con.createStatement();
    st.execute(sqlText);
    st.close();
  }

  private void trigerDownStreamJob(Connection con) {
    String sys = ((JobStepInfo)this.listStep.get(this.stepCurrent)).sys;
    String job = ((JobStepInfo)this.listStep.get(this.stepCurrent)).job;
    String txDate = ((JobStepInfo)this.listStep.get(this.stepCurrent)).txDate;
    if (this.retCode != 0) return;
    String sqlText = "INSERT INTO ETL_Job_Queue(ETL_System, ETL_Job,ETL_Server,TXDate,RequestTime, SeqID) SELECT B.ETL_System, B.ETL_Job, ETL_Server, CAST('" + 
      txDate + "' AS DATE FORMAT 'YYYYMMDD'), '" + 
      ETL.GetDateTime() + "', 0 " + 
      " FROM ETL_Job_Stream A, ETL_JOB B " + 
      " WHERE A.ETL_System='" + sys + "' AND A.ETL_Job='" + job + "' AND A.Enable='1' " + 
      "  AND A. Stream_System = B.ETL_System AND A.Stream_Job = B.ETL_Job ";
    try {
      Statement st = con.createStatement();
      st.execute(sqlText);
      int streamCount = st.getUpdateCount();
      st.close();
      this.slaveLog = (this.slaveLog + streamCount + " DownStream Job has been trigered.\n");
    } catch (SQLException e) {
      e.printStackTrace();
      System.err.print(sqlText);
    }
  }

  public static int submitFisrtStep(Connection con, String sys, String job, String ctlFile)
  {
    Properties prop = new Properties();
    try {
      prop.load(new FileInputStream(ETL.cfgInit)); } catch (FileNotFoundException localFileNotFoundException) {
    } catch (IOException localIOException) {
    }
    prop.setProperty("SYS", sys); prop.setProperty("JOB", job);
    prop.setProperty("CTLFILE", ctlFile);
    String txdate = ctlFile.substring(ctlFile.length() - 12, ctlFile.length() - 4);
    prop.setProperty("TXDATE", txdate);
    try {
      Vector sList = JobStepInfo.getStep(con, sys, job, prop);
      if (sList.size() == 0) return 1;
      JobStepInfo firstStep = (JobStepInfo)sList.firstElement();
      firstStep.updateStepRunning(con, 0);
      new JobRunner(sList, prop);
      return 0;
    } catch (SQLException e) {
      System.out.println("Submit JOB[" + sys + "," + job + "]" + e.getMessage());
      e.printStackTrace();
    }
    return 2;
  }

  private void doSlave()
  {
    setName("Slave");
    String lastDate = " ";
    Connection con = ETL.Connect();
    while (true) try { JobRunner t;
        do { Calendar c = Calendar.getInstance();
          String dateStr = String.format("%1$tY%1$tm%1$td", new Object[] { c });
          if (!dateStr.equals(lastDate)) {
            lastDate = dateStr;
            String fileName = ETL.Auto_home + "/LOG/etlslave_" + dateStr + ".log";
            try {
              slaveLogF = new PrintStream(new FileOutputStream(fileName, true));
            } catch (FileNotFoundException e) {
              e.printStackTrace(); slaveLogF = System.out;
            }
            ETL.PrintVersionInfo(slaveLogF, "DWAuto-SLave");
          }
          t = (JobRunner)jobFinishQueue.poll(1L, TimeUnit.MINUTES);
          if ((t == null) && (ETL.Stop_Flag))
            return; }

        while (t == null);

        if (!ETL.ping(con)) try {
            con.close(); con = null; } catch (SQLException e) { con = null; }
        while (con == null) {
          con = ETL.Connect();
          if (con != null) continue; ETL.Sleep(30);
        }
        t.processNextStep(con); } catch (InterruptedException localInterruptedException) {
      } 
  }

  public void run() {
    if (this.stepCmd == null) {
      if (this.AgentPort == 1) doSlave();
      if (this.AgentPort > 1024) doAgent(this.AgentPort);
      return;
    }

    setName(((JobStepInfo)this.listStep.get(this.stepCurrent)).sys + "/" + ((JobStepInfo)this.listStep.get(this.stepCurrent)).job);

    this.startDateTime = ETL.GetDateTime();
    this.retLog = "";
    if (this.stepCmd.equals("V")) {
      this.retLog = "Virtual Job has no job step.\n";
      this.slaveLog += "This is a Virtual Job\n";
      this.retCode = 0;
    } else {
      doStep();
    }this.endDateTime = ETL.GetDateTime();
    this.slaveLog = (this.slaveLog + "RetCode=" + this.retCode + "\n");
    try {
      jobFinishQueue.put(this);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void doStep() {
    String[] args = this.stepCmd.split(" +");
    ArrayList cmd = new ArrayList();
    for (int i = 0; i < args.length; ++i) cmd.add(args[i]);
    ProcessBuilder pb = new ProcessBuilder(cmd);
    Map env = pb.environment();
    Enumeration keyE = this.prop.keys();
    while (keyE.hasMoreElements()) {
      String key1 = (String)keyE.nextElement();
      if (key1.startsWith("AUTO")) {
        env.put(key1, this.prop.getProperty(key1));
      }
    }
    File dirF = new File(this.workDir); if (dirF.exists()) pb.directory(new File(this.workDir));
    pb.redirectErrorStream(true);
    try {
      this.slaveLog = (this.slaveLog + "run: " + this.stepCmd + " //Work Path=" + this.workDir + "\n");
      this.processWork = pb.start();
      BufferedReader jobOut = new BufferedReader(new InputStreamReader(this.processWork.getInputStream()));
      try
      {
        JobStepInfo s = (JobStepInfo)this.listStep.elementAt(this.stepCurrent);
        String dirName = ETL.Auto_home + "/LOG/" + s.sys + "/" + this.startDate;
        File logDir = new File(dirName); if (!logDir.exists()) logDir.mkdirs();
        s.logName = (dirName + "/" + s.job + "_" + s.sessionId + "_" + s.Step_No + ".log");
        PrintStream logFile = new PrintStream(new FileOutputStream(new File(s.logName)));
        String line;
        while ((line = jobOut.readLine()) != null)
        {
          String line;
          this.retLog = (this.retLog + line + "\n");
          logFile.println(line);
        }
        logFile.close();
        jobOut.close();
        try {
          this.processWork.waitFor();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        this.retCode = this.processWork.exitValue(); } catch (IOException localIOException1) {
      }
    } catch (IOException e) {
      this.retLog += e.getMessage();
    }
  }

  private void doAgent(int tcpPort) {
    try {
      setName("Agent");
      ServerSocket serverSock = new ServerSocket(tcpPort);
      try {
        Connection con = null;

        PrintStream agtLog = null;
        Socket clientSock = serverSock.accept();
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
        PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSock.getOutputStream()));
        String agentCmd = in.readLine();
        String[] words = agentCmd.split(" ");
        String outText = "-ERR\nUnknown Command or error paramenters.";
        JobRunner job = null;
        Calendar c = Calendar.getInstance();
        String dateStr = String.format("%1$tY%1$tm%1$td", new Object[] { c });
        String fileName = ETL.Auto_home + "/LOG/etlagent_" + dateStr + ".log";
        try {
          agtLog = new PrintStream(new FileOutputStream(fileName, true));
        } catch (FileNotFoundException e) {
          e.printStackTrace(); agtLog = System.out;
        }
        SocketAddress req = clientSock.getRemoteSocketAddress();
        String reqAddress = req.toString();
        ETL.ShowTime(agtLog);
        agtLog.println("From: " + reqAddress + " CMD:" + agentCmd + " " + words.length);
        agtLog.println("      " + agentCmd);
        if ((words.length == 1) && (words[0].equals("STOP")) && 
          (reqAddress.startsWith("/127."))) {
          ETL.Stop_Flag = true;
          outText = "-OK\nAutomation Shutdown.";
        }

        if (words.length >= 3) synchronized (runningJob) {
            int lengthRunningJob = runningJob.size();
            for (int i = 0; i < lengthRunningJob; ++i) {
              if (((JobRunner)runningJob.get(i)).getName().equals(words[1] + "/" + words[2])) {
                job = (JobRunner)runningJob.get(i);
                break;
              }
            }
          }
        if (words[0].equalsIgnoreCase("QSERVERSTATUS")) {
          if (words[1].equalsIgnoreCase("HOLD"))
            ETL.serviceHold = 1;
          else if (words[1].equalsIgnoreCase("RELEASE")) {
            ETL.serviceHold = 0;
          }
          Thread[] threads = new Thread[Thread.activeCount()];
          int numThreads = Thread.enumerate(threads);
          String h = (ETL.serviceHold == 1) ? "Hold" : "Normal";
          outText = "+OK\nService Status:" + h + "\n-----------------\n";
          for (int i = 0; i < numThreads; ++i) {
            if (threads[i].getName().equalsIgnoreCase("process reaper"))
              continue;
            outText = outText + threads[i].getName() + "\t" + threads[i].getState() + "\n";
          }
        } else if ((words.length == 3) && (words[0].equalsIgnoreCase("QUERYLOG"))) {
          if (job == null)
            outText = "-ERR\nRealtime running Job is not find, Maybe the Job is finished\n";
          else
            outText = "+OK\n" + job.slaveLog + job.retLog;
        }
        else if ((words[0].equals("FORCE")) && (words.length == 4)) {
          if (job != null) {
            outText = "-ERR\nThe job is running in the ETL Server\n";
          } else {
            if ((!ETL.ping(con)) && (con != null)) try {
                con.close(); con = null; } catch (SQLException e) { con = null; }
            while (con == null) {
              con = ETL.Connect();
              if (con != null) continue; ETL.Sleep(30);
            }
            int rc = submitFisrtStep(con, words[1], words[2], words[3]);
            if (rc == 0)
              outText = "+OK\nThe Job is started by force.";
            else if (rc == 1)
              outText = "-ERR\nThe Job has no job step.\n";
          }
        }
        else if ((words[0].equals("VIEWSCRIPT")) && (words.length == 4)) {
          Properties prop = new Properties();
          try {
            prop.load(new FileInputStream(ETL.cfgInit));
            StringBuffer x = new StringBuffer("+OK\n");
            String fName = ETL.putVarsToCommand(prop, words[3]);
            try {
              BufferedReader txtFile = new BufferedReader(new InputStreamReader(new FileInputStream(fName)));
              while (true) {
                String line = txtFile.readLine();
                if (line == null) break;
                x.append(line + "\n");
              }
              outText = new String(x);
            } catch (IOException e) {
              outText = "-ERR\n Open File(" + fName + ") failed!!!";
            }
          } catch (FileNotFoundException e1) {
            outText = "-ERR\n Automation System Error!!!";
          }
          catch (IOException e1) {
            outText = "-ERR\n Automation System Error!!!";
          }
        } else if ((words[0].equals("RETRY")) && (words.length == 4)) {
          if (job != null) {
            outText = "-ERR\nThe Job is running.\n";
          } else {
            if ((!ETL.ping(con)) && (con != null)) try {
                con.close(); con = null; } catch (SQLException e) { con = null; }
            while (con == null) {
              con = ETL.Connect();
              if (con != null) continue; ETL.Sleep(30);
            }
            int rc = retryJob(con, words[1], words[2], words[3]);
            if (rc == 0)
              outText = "+OK\nThe Job was put into Queue.\n";
            else if (rc == 1) {
              outText = "-ERR\nThe Job has no job step.\n";
            }
          }
        }
        agtLog.close();
        out.print(outText); out.close();
        clientSock.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (IOException e) {
      System.err.print("The port " + tcpPort + " Current in use.");
      System.exit(0);
    }
  }

  private int retryJob(Connection con, String sys, String job, String ctlf) {
    String sql = "SEL COUNT(*) FROM ETL_Job_Step WHERE  ETL_System='" + sys + 
      "' AND ETL_Job='" + job + "' AND Enable = '1'";
    int countStep = 0;
    try {
      Statement st = con.createStatement();
      ResultSet rs = st.executeQuery(sql);
      rs.next(); countStep = rs.getInt(1);
      st.close();
      if (countStep == 0) return 1;
      sql = "SEL JobSess, Location FROM ETL_Received_File A, (SEL ETL_System, ETL_job, JobSessionID-1 JobSess FROM etl_job WHERE ETL_System = '" + 
        sys + "' AND ETL_Job = '" + job + "') B  " + 
        "WHERE A.ETL_System = B.ETL_System  AND A.ETL_Job = B.ETL_Job AND JobSessionID = JobSess " + 
        "GROUP BY 1,2 ";
      rs = st.executeQuery(sql);
      int sID = 0; String loc = "";
      if (rs.next()) { sID = rs.getInt(1); loc = rs.getString(2); }
      st.close();
      if (loc.length() != 0) {
        int datePos = ctlf.length() - 12;
        String pJob = sys + "\t" + job + "\t" + ctlf.substring(datePos, datePos + 8);
        File ctlFile = Master.getControlFile(con, pJob, loc);
        ContentControl ct = new ContentControl(ctlFile.getPath());
        ct.MoveTo(ETL.Auto_home + "/DATA/queue");
        ct.updateFileLocation(con, ETL.Auto_home + "/DATA/queue", sys, job, sID);
      }
      sql = String.format("UPDATE ETL_Job SET Last_JobStatus = 'Pending' WHERE ETL_System = '%1$s' AND ETL_Job = '%2$s'", new Object[] { 
        sys, job });
      st.execute(sql);
      st.close();
      return 0;
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return 2;
  }

  public static void main(String[] args) {
    ETL.Initialize("d:/DWETL/etc/autocfg.txt");

    new JobRunner(1);
    new JobRunner(7078);

    System.out.print("Submit finished.");
    ETL.Sleep(10);
    while (true) {
      int inChar = 32;
      System.out.print("Running Job: " + RunningCount.get() + "\n");
      try {
        if (System.in.available() > 0) inChar = System.in.read(); else
          ETL.Sleep(5); 
      } catch (IOException localIOException) {
      }
      if (inChar == 65) {
        JobRunner job = (JobRunner)runningJob.get(0);
        job.processWork.destroy();
      }
      if (inChar == 81);
      System.exit(0);
    }
  }
}
