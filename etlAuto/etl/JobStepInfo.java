package etl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Vector;
import java.util.zip.GZIPOutputStream;

class JobStepInfo
{
  public String logName;
  String sys;
  String job;
  String txDate;
  int sessionId;
  int Step_No;
  String Step_type;
  String OSProgram;
  String WorkDir;
  String ScriptName;
  String ScriptPath;
  String AdditionParameters;
  String exeCommand;
  int logContentID;
  private String sTime;

  String StepCommand(Properties props)
  {
    String cmds;
    String cmds;
    if (this.ScriptName.length() > 0)
      cmds = this.OSProgram + " " + this.ScriptPath + "/" + this.ScriptName + " $CTLFILE " + this.AdditionParameters;
    else {
      cmds = this.OSProgram + " " + this.AdditionParameters;
    }
    return ETL.putVarsToCommand(props, cmds);
  }
  public void updateRecordLog(Connection con, String retLog) {
    if (!this.Step_type.equals("L")) return;
    int num_ins = 0; int num_dup = 0; int num_upd = 0; int num_del = 0; int num_out = 0;
    int num_er1 = 0; int num_er2 = 0; int num_et = 0; int num_uv = 0;
    boolean loadTypeFind = true;
    String loadTool = "";
    for (int pos = 0; pos < retLog.length(); ) {
      int endLine = retLog.indexOf('\n', pos);
      if (endLine == -1) break;
      String line = retLog.substring(pos, endLine);
      pos = endLine + 1;
      if (loadTypeFind) {
        if ((line.indexOf("UTILITY") > 0) || (line.indexOf("Utility") > 0)) {
          String[] w = line.split(" +");
          if ((w.length <= 4) || (!w[3].equalsIgnoreCase("UTILITY"))) continue;
          loadTool = w[2];

          if ((!loadTool.equalsIgnoreCase("FASTLOAD")) && (!loadTool.equalsIgnoreCase("MultiLoad"))) continue;
          loadTypeFind = false;

          continue;
        }if (line.startsWith("     =              LOAD OPERATOR")) {
          loadTool = "TPT-LOADOP";
          loadTypeFind = false;
        }
      }
      try {
        if (loadTool.equals("FASTLOAD")) {
          if (line.indexOf("Total Inserts Applied") > 0)
            num_ins = Integer.parseInt(line.split(" +")[5]);
          else if (line.indexOf("Total Duplicate Rows") > 0)
            num_dup = Integer.parseInt(line.split(" +")[5]);
          else if (line.indexOf("Total Error Table 1") > 0)
            num_er1 = Integer.parseInt(line.split(" +")[6]);
          else if (line.indexOf("Total Error Table 2") > 0)
            num_er2 = Integer.parseInt(line.split(" +")[6]);
        }
        else if (loadTool.equals("MultiLoad"))
          if (line.substring(0, 16).equals("        Inserts:")) {
            num_ins = Integer.parseInt(line.split(" +")[2]);
          } else if (line.substring(0, 16).equals("        Updates:")) {
            num_upd = Integer.parseInt(line.split(" +")[2]);
          } else if (line.substring(0, 16).equals("        Deletes:")) {
            num_del = Integer.parseInt(line.split(" +")[2]);
          } else if (line.substring(0, 37).equals("     Number of Rows  Error Table Name")) {
            String[] LineError = retLog.substring(pos).split("\n");
            num_et = Integer.parseInt(LineError[1].split(" +")[1]);
            num_uv = Integer.parseInt(LineError[2].split(" +")[1]);
          }
        else if (loadTool.equals("TPT-LOADOP"))
          if (line.startsWith("              Total Rows Applied:")) {
            num_ins = Integer.parseInt(line.split(" +")[4]);
          } else if (line.startsWith("              Total Possible Duplicate Rows:")) {
            num_dup = Integer.parseInt(line.split(" +")[5]);
          } else if (line.equals("     Number of rows  Error Table Name")) {
            endLine = retLog.indexOf('\n', pos);
            if (endLine == -1) break label690;
            pos = endLine + 1;
            endLine = retLog.indexOf('\n', pos);
            if (endLine == -1) break label690;
            line = retLog.substring(pos, endLine);
            pos = endLine + 1;
            num_er1 = Integer.parseInt(line.split(" +")[1]);
            endLine = retLog.indexOf('\n', pos);
            if (endLine == -1) break label690;
            line = retLog.substring(pos, endLine);
            pos = endLine + 1;
            num_er2 = Integer.parseInt(line.split(" +")[1]);
          }
      }
      catch (NumberFormatException localNumberFormatException)
      {
      }
    }
    if ((num_dup > 0) || (num_et > 0) || (num_uv > 0) || (num_er1 > 0) || (num_er2 > 0))
    {
      label690: String content = "Duplicate Record = " + num_dup + "\n" + 
        "Multiload ET record = " + num_et + "\n" + 
        "Multiload UV record = " + num_uv + "\n" + 
        "Fastload ER1 record = " + num_er1 + "\n" + 
        "Fastload ER2 record = " + num_er2 + "\n";
      String desc = "Job [" + this.sys + "," + this.job + "] has error record";
      ETL.WriteMessageNotification(this.sys, this.job, this.txDate, "RecordError", desc, content);
    }
    String sql = "DELETE FROM ETL_Record_Log WHERE ETL_System ='" + this.sys + "' AND ETL_Job = '" + this.job + 
      "' AND JobSessionID = " + this.sessionId + 
      "; INSERT INTO ETL_Record_Log (ETL_System, ETL_Job, JobSessionID, RecordTime, " + 
      "        InsertedRecord, UpdatedRecord, DeletedRecord," + 
      "        DuplicateRecord, OutputRecord, ETRecord, UVRecord," + 
      "        ER1Record, ER2Record)" + 
      " VALUES ('" + this.sys + "', '" + this.job + "', " + this.sessionId + ", '" + ETL.GetDateTime() + "', " + 
      num_ins + ", " + num_upd + ", " + num_del + ", " + num_dup + ", " + num_out + ", " + 
      num_et + ", " + num_uv + ", " + num_er1 + ", " + num_er2 + ");";
    try {
      Statement st = con.createStatement();
      st.execute(sql);
      st.close(); } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  public void updateStepRunning(Connection con, int s) {
    try {
      Statement st = con.createStatement();
      String setStartT = "";
      String sqlText = "";
      if (s == 0) {
        setStartT = " ,Last_StartTime='" + ETL.GetDateTime() + "', Last_EndTime=null, " + 
          "Last_JobStatus='Running', Last_TXDate = CAST('" + this.txDate + "' AS DATE FORMAT 'YYYYMMDD')";

        sqlText = "UPDATE ETL_Job_GroupChild SET CheckFlag = 'N' WHERE ETL_System = '" + this.sys + "' AND ETL_Job = '" + this.job + "';";
      }
      sqlText = sqlText + "Update ETL_job SET RunningScript='" + this.ScriptName + "'" + setStartT + " WHERE ETL_System='" + this.sys + 
        "' AND ETL_Job='" + this.job + "';";
      this.sTime = ETL.GetDateTime();
      sqlText = sqlText + "Insert INTO ETL_Job_Log(ETL_System, ETL_Job, JobSessionID, ScriptFile, TxDate, StartTime, Step_No) SEL ETL_System, ETL_Job, JobSessionID, RunningScript, Last_TXDate, '" + 
        this.sTime + "', " + this.Step_No + 
        " FROM ETL_Job WHERE ETL_System='" + this.sys + "' AND ETL_Job='" + this.job + "'";
      st.execute(sqlText);
      st.close(); } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  public void updateStepFinal(Connection con, int retCode, String eTime, String logContent) {
    try {
      File tmpZipFile = File.createTempFile("zip" + this.job, ".gz", new File(ETL.Auto_home + "/LOG"));
      BufferedOutputStream out = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(tmpZipFile)));
      out.write(logContent.getBytes());
      out.close();

      FileInputStream zipLog = new FileInputStream(tmpZipFile);
      String sqlText = "Update ETL_Job_Log SET EndTime='" + eTime + "', ReturnCode=" + retCode + 
        ", LogContent = ? WHERE ETL_System='" + this.sys + 
        "' AND ETL_Job='" + this.job + "' AND JobSessionID=" + this.sessionId + " AND StartTime='" + this.sTime + 
        "' AND Step_No=" + this.Step_No;
      PreparedStatement st = con.prepareStatement(sqlText);

      st.setBinaryStream(1, zipLog, (int)tmpZipFile.length());
      try {
        st.executeUpdate();
      } catch (SQLException e) {
        e.printStackTrace();
        System.err.println("sqlcode is " + e.getErrorCode());
        System.err.println("len of logContent is " + logContent.length());
        System.err.println("len of zipFile is " + tmpZipFile.length());
        System.err.println("sqlText " + sqlText);
      }
      zipLog.close();
      st.close();

      tmpZipFile.delete();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static Vector<JobStepInfo> getStep(Connection con, String sys, String job, Properties prop) throws SQLException {
    Vector stepList = new Vector();
    Statement st = con.createStatement();
    String sqlText = "SEL JobType, JobSessionID FROM ETL_Job WHERE ETL_System = '" + 
      sys + "' AND ETL_Job = '" + job + "'";
    ResultSet rs = st.executeQuery(sqlText);

    String jobType = "";
    int sessionId = 0;
    String txDate = prop.getProperty("TXDATE");
    if (rs.next()) {
      jobType = rs.getString(1);
      sessionId = rs.getInt(2);
    }
    rs.close();
    if (jobType.equalsIgnoreCase("V")) {
      JobStepInfo s = new JobStepInfo(); s.sys = sys; s.job = job;
      s.txDate = txDate; s.sessionId = sessionId;
      s.OSProgram = "VirtualPGM";
      s.ScriptName = "Virtual-Script";
      s.exeCommand = "V";
      s.Step_type = "V";
      stepList.add(s);
      return stepList;
    }
    sqlText = "SELECT Step_No, Step_type, OSProgram, WorkDir, ScriptName, ScriptPath, AdditionParameters FROM ETL_Job_Step WHERE Enable = '1' AND ETL_System = '" + 
      sys + "' AND ETL_Job = '" + job + 
      "' ORDER BY Step_NO";
    rs = st.executeQuery(sqlText);
    while (rs.next()) {
      JobStepInfo s = new JobStepInfo(); s.sys = sys; s.job = job; s.txDate = txDate; s.sessionId = sessionId;
      s.Step_No = rs.getInt(1);
      s.Step_type = rs.getString(2);
      s.OSProgram = rs.getString(3);
      s.WorkDir = rs.getString(4); if (rs.wasNull()) s.WorkDir = "";
      if (s.WorkDir.length() == 0)
        s.WorkDir = "$AUTO_HOME/DATA/process";
      s.ScriptName = rs.getString(5); if (rs.wasNull()) s.ScriptName = "";
      s.ScriptPath = rs.getString(6); if (rs.wasNull()) s.ScriptPath = "";
      if (s.ScriptPath.length() == 0)
        s.ScriptPath = ("$AUTO_HOME/APP/" + sys + "/" + job + "/bin");
      s.AdditionParameters = rs.getString(7);
      if (rs.wasNull()) s.AdditionParameters = "";
      s.exeCommand = s.StepCommand(prop);
      s.WorkDir = ETL.putVarsToCommand(prop, s.WorkDir);
      stepList.add(s);
    }
    rs.close(); st.close();
    return stepList;
  }
}
