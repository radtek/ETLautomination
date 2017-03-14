package etl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class JobInfo
{
  public String sysName;
  public String jobName;
  public String convHead;
  public String checkLastStatus;
  public String enable;
  public String jobSessionID;
  public String jobStatus;
  public String autoFilter;
  public String txDate;
  public String frequency;
  public String checkCalendar;
  public String jobType;

  JobInfo(Connection con, String jobSource)
  {
    String sql = "SELECT A.etl_system, A.etl_job, B.Conv_File_Head, A.CheckLastStatus, A.Enable, A.JobSessionID, A.Last_JobStatus, B.AutoFilter,A.Last_TXDate, A.Frequency, A.CheckCalendar, A.JobType FROM etl_job A, etl_job_source B WHERE A.etl_system=B.etl_system AND A.etl_job=B.etl_job AND B.source ='" + 
      jobSource + "'";
    try {
      Statement st = con.createStatement();
      ResultSet rs = st.executeQuery(sql);
      if (rs.next()) {
        this.sysName = rs.getString(1);
        this.jobName = rs.getString(2);
        this.convHead = rs.getString(3);
        this.checkLastStatus = rs.getString(4); if (rs.wasNull()) this.checkLastStatus = "";
        this.enable = rs.getString(5); if (rs.wasNull()) this.enable = "";
        this.jobSessionID = rs.getString(6); if (rs.wasNull()) this.jobSessionID = "0";
        this.jobStatus = rs.getString(7); if (rs.wasNull()) this.jobStatus = "";
        this.autoFilter = rs.getString(8); if (rs.wasNull()) this.autoFilter = "";
        this.txDate = rs.getString(9); if (rs.wasNull()) this.txDate = ""; this.txDate = this.txDate.replaceAll("-", "");
        this.frequency = rs.getString("Frequency"); if (rs.wasNull()) this.frequency = "";
        this.checkCalendar = rs.getString("CheckCalendar"); if (rs.wasNull()) this.checkCalendar = "N";
        this.jobType = rs.getString("JobType"); if (rs.wasNull()) this.jobType = "D";
      }
      rs.close(); st.close(); } catch (SQLException ex) {
      RCV.dberr = true; ex.printStackTrace();
    }
  }
}
