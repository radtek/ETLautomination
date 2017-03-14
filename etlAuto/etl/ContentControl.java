package etl;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ContentControl
{
  public String nameControl;
  public short dataCount;
  public String[] dataFile;
  public long[] fileSize;
  public long[] expect;
  public long totalExpectedRecord;

  public ContentControl(String dir, String name)
  {
    this(dir + "/" + name);
  }
  public ContentControl(String pathControl) {
    this.dataCount = 0;
    this.nameControl = pathControl;
    File ctl = new File(this.nameControl);
    if (!ctl.exists()) return;
    if (ctl.length() == 0L) return;
    String txtControl = "";
    try {
      DataInputStream rf = new DataInputStream(new FileInputStream(this.nameControl));
      byte[] byteTxt = new byte[(int)ctl.length()];
      int rb = rf.read(byteTxt);
      txtControl = (rb > 0) ? new String(byteTxt, 0, rb) : "";
      rf.close(); } catch (IOException localIOException) {
    }
    String[] lines = txtControl.split("[\n\r]+");
    this.dataCount = (short)lines.length;
    this.dataFile = new String[this.dataCount];
    this.fileSize = new long[this.dataCount];
    this.expect = new long[this.dataCount];
    this.totalExpectedRecord = 0L; this.dataCount = 0;
    for (int i = 0; i < lines.length; ++i) {
      this.dataFile[i] = "";
      String[] fields = lines[i].split("\\s+");
      if (fields.length >= 2) {
        this.dataFile[i] = fields[0]; this.fileSize[i] = 0L; this.expect[i] = 0L;
        if ((fields.length > 1) && (fields[1].matches("[0-9]+")))
          this.fileSize[i] = Long.parseLong(fields[1]);
        if ((fields.length > 2) && (fields[2].matches("[0-9]+")))
          this.expect[i] = Long.parseLong(fields[2]);
        this.totalExpectedRecord += this.expect[i];
        this.dataCount = (short)(this.dataCount + 1);
      }
    }
  }

  public void MoveTo(String toDir) {
    File t = new File(toDir);
    if (!t.exists()) t.mkdirs();
    File f = new File(this.nameControl);
    if (!f.exists()) return;
    for (int i = 0; i < this.dataCount; ++i) {
      t = new File(f.getParent() + "/" + this.dataFile[i]);
      File t1 = new File(toDir + "/" + this.dataFile[i]);
      if (t1.exists()) t1.delete(); t.renameTo(t1);
    }
    t = new File(toDir + "/" + f.getName());
    if (t.exists()) t.delete(); f.renameTo(t);
  }

  public boolean CheckDataFileSize() {
    File f = new File(this.nameControl);
    for (int i = 0; i < this.dataCount; ++i) {
      File f1 = new File(f.getParent() + "/" + this.dataFile[i]);
      if (f1.length() != this.fileSize[i])
        return false;
    }
    return true;
  }
  public void MoveToday(String toDir) {
    MoveTo(toDir + "/" + ETL.today);
  }

  public void updateFileLocation(Connection con, String loc, String sys, String job)
  {
    try
    {
      for (int i = 0; i < this.dataCount; ++i) {
        Statement st = con.createStatement();
        String sqlText = "UPDATE ETL_Received_File SET Location = '" + loc + 
          "'   WHERE ETL_System = '" + sys + "' AND ETL_Job = '" + job + 
          "'   AND ReceivedFile = '" + this.dataFile[i] + "'";
        st.execute(sqlText);
        st.close();
      }
    } catch (SQLException localSQLException) {
    }
  }

  public void updateFileLocation(Connection con, String loc, String sys, String job, int sess) {
    try {
      for (int i = 0; i < this.dataCount; ++i) {
        Statement st = con.createStatement();
        String sqlText = "UPDATE ETL_Received_File SET JobSessionID = " + sess + "+1, Location = '" + loc + 
          "'   WHERE ETL_System = '" + sys + "' AND ETL_Job = '" + job + 
          "'   AND ReceivedFile = '" + this.dataFile[i] + "'";
        st.execute(sqlText);
        st.close();
      }
    } catch (SQLException localSQLException) {
    }
  }

  public boolean CheckDataFileDuplicate(Connection con, String sysName, String jobName, PrintStream log) {
    boolean ret = true;
    if (this.dataCount == 0) return ret;

    String sql = "SELECT ReceivedFile FROM ETL_Received_File WHERE ETL_System = '" + sysName + 
      "' AND ETL_Job = '" + jobName + "' AND ReceivedFile = ? ";
    try {
      PreparedStatement st = con.prepareStatement(sql);
      for (int i = 0; i < this.dataCount; ++i) {
        st.setString(1, this.dataFile[i]);
        ResultSet rs = st.executeQuery();
        if (rs.next()) {
          ETL.ShowPrefixSpace(log); log.format("The source file '%1$s' is already received!", new Object[] { this.dataFile[i] });
          ret = false;
          String eventDesc = "[" + sysName + "], [" + jobName + "] has received duplicate file " + this.dataFile;
          ETL.InsertEventLog(con, "RCV", "M", eventDesc);
        }
        rs.close();
      }
      st.close(); } catch (SQLException localSQLException) {
    }
    return ret;
  }
}
