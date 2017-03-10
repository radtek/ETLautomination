package etl;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;

class ETLSys
{
  String sys;
  int DataKeepPeriod;
  int LogKeepPeriod;
  int RecordKeepPeriod;

  static ArrayList<ETLSys> dbGetSys(Connection con)
  {
    ArrayList list = new ArrayList();
    String sql = "SEL ETL_System, DataKeepPeriod , LogKeepPeriod, RecordKeepPeriod FROM ETL_Sys";
    try {
      Statement s = con.createStatement();
      ResultSet r = s.executeQuery(sql);
      while (r.next()) {
        ETLSys e = new ETLSys();
        e.sys = r.getString(1);
        e.DataKeepPeriod = r.getInt(2); if (r.wasNull()) e.DataKeepPeriod = 30;
        e.LogKeepPeriod = r.getInt(3); if (r.wasNull()) e.LogKeepPeriod = 30;
        e.RecordKeepPeriod = r.getInt(4); if (r.wasNull()) e.RecordKeepPeriod = 30;
        list.add(e);
      }
      r.close(); s.close(); } catch (SQLException e) {
      HouseKeeping.dberr = true;
    }return list;
  }

  public void cleaup(Connection con) {
    Calendar c = Calendar.getInstance(); long current = System.currentTimeMillis();

    c.setTimeInMillis(current - this.DataKeepPeriod * 24 * 3600 * 1000L);
    String expiredDate = String.format("%1$tY%1$tm%1$td", new Object[] { c });

    HouseKeeping.clearupPath(ETL.Auto_home + "/DATA/complete/" + this.sys, expiredDate);
    HouseKeeping.clearupPath(ETL.Auto_home + "/DATA/fail/corrupt/" + this.sys, expiredDate);
    HouseKeeping.clearupPath(ETL.Auto_home + "/DATA/fail/bypass/" + this.sys, expiredDate);
    HouseKeeping.clearupPath(ETL.Auto_home + "/DATA/fail/duplicate/" + this.sys, expiredDate);
    HouseKeeping.clearupPath(ETL.Auto_home + "/DATA/fail/error/" + this.sys, expiredDate);

    c.setTimeInMillis(current - this.LogKeepPeriod * 24 * 3600 * 1000L);
    expiredDate = String.format("%1$tY%1$tm%1$td", new Object[] { c });
    HouseKeeping.clearupPath(ETL.Auto_home + "/LOG/" + this.sys, expiredDate);

    c.setTimeInMillis(current - this.RecordKeepPeriod * 24 * 3600 * 1000L);
    expiredDate = String.format("%1$tY-%1$tm-%1$td", new Object[] { c });
    if (ETL.isPrimaryServer) {
      ETL.ShowTime(HouseKeeping.log);
      HouseKeeping.log.println("Clean repository log for system '" + this.sys + "'");
      try {
        String sql = "DELETE FROM ETL_Received_File WHERE ETL_System = '" + this.sys + 
          "' AND ReceivedTime <= '" + expiredDate + "'";
        Statement st = con.createStatement();
        st.execute(sql); st.close();
        sql = "DELETE FROM ETL_Record_Log WHERE ETL_System = '" + this.sys + 
          "' AND RecordTime <= '" + expiredDate + "'";
        st.execute(sql); st.close();

        sql = "DELETE FROM ETL_Job_Log WHERE ETL_System ='" + this.sys + 
          "' AND StartTime <= '" + expiredDate + "'";
        st.execute(sql); st.close(); } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}
