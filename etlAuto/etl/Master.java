/*     */ package etl;
/*     */ 
/*     */ import java.io.File;
/*     */ import java.io.FileNotFoundException;
/*     */ import java.io.FileOutputStream;
/*     */ import java.io.IOException;
/*     */ import java.io.PrintStream;
/*     */ import java.sql.Connection;
/*     */ import java.sql.PreparedStatement;
/*     */ import java.sql.ResultSet;
/*     */ import java.sql.SQLException;
/*     */ import java.sql.Statement;
/*     */ import java.util.Calendar;
/*     */ import java.util.Vector;
/*     */ import java.util.concurrent.atomic.AtomicInteger;
/*     */ 
/*     */ public class Master
/*     */ {
/*     */   static Connection con;
/*  10 */   static boolean dberr = false;
/*  11 */   private static String lastRunTime = "-1";
/*     */ 
/*     */   private static Vector<String> getPendingJob()
/*     */   {
/*  16 */     String selPendingJob = 
/*  17 */       "SELECT\tB.etl_system, B.etl_job, (B.Last_TXDate(INTEGER))+19000000 , B.JobType\t\nFROM ETL_Job_TimeWindow A, ETL_Job B                                                           \nWHERE                                                                                          \n   A.etl_system = b.etl_system AND a.etl_job = b.etl_job AND Last_JobStatus = 'Pending' AND    \n   B.Enable = '1' AND B.ETL_Server = '" + 
/*  21 */       ETL.Auto_server + "' AND \n" + 
/*  23 */       "\t( CASE\tWHEN %1$d>=beginhour AND\t%1$d<=endhour THEN allow                                      \n" + 
/*  24 */       "\t     \tWHEN\t(beginhour >= endhour ) AND\t( %1$d>=beginhour OR\t%1$d <= endhour) THEN allow\t\n" + 
/*  25 */       "\t\t    ELSE CASE allow \tWHEN\t'Y' THEN 'N' \tELSE\t'Y' END\t                        \n" + 
/*  26 */       "\t  END )  = 'Y'                                                                              \n" + 
/*  27 */       "ORDER BY B.Job_Priority DESC, B.Last_TXDate, B.Last_StartTime";
/*  28 */     Calendar c = Calendar.getInstance(); c.setTimeInMillis(System.currentTimeMillis());
/*  29 */     int hour = c.get(11);
/*  30 */     String sqlText = String.format(selPendingJob, new Object[] { Integer.valueOf(hour) });
/*  31 */     Vector ret = new Vector();
/*     */     try {
/*  33 */       Statement st = con.createStatement();
/*  34 */       ResultSet rs = st.executeQuery(sqlText);
/*  35 */       while (rs.next()) {
/*  36 */         String rowStr = rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\t" + rs.getString(4);
/*  37 */         ret.add(rowStr);
/*     */       }
/*  39 */       rs.close(); st.close(); } catch (SQLException ex) {
/*  40 */       dberr = true;
/*  41 */     }return ret;
/*     */   }
/*     */ 
/*     */   static File getControlFile(Connection con, String pendingJob, String path) {
/*  45 */     File cf = null;
/*  46 */     String[] w = pendingJob.split("\t");
/*  47 */     String sqlText = "SEL Conv_File_Head FROM ETL_Job_Source WHERE ETL_System ='" + w[0] + 
/*  48 */       "' AND ETL_Job = '" + w[1] + "'";
/*     */     try
/*     */     {
/*  51 */       Statement st = con.createStatement();
/*  52 */       ResultSet rs = st.executeQuery(sqlText);
/*  53 */       while (rs.next()) {
/*  54 */         String convSrc = rs.getString(1);
/*  55 */         String ctlFile = w[0] + "_" + convSrc + "_" + w[2] + ".dir";
/*  56 */         cf = new File(path + "/" + ctlFile);
/*  57 */         if (cf.exists()) break;
/*     */       }
/*  59 */       rs.close(); st.close();
/*     */     } catch (SQLException e) {
/*  61 */       e.printStackTrace();
/*     */     }
/*  63 */     return cf;
/*     */   }
/*     */ 
/*     */   private static boolean checkJobDependency(String sys, String job, String txdate, PrintStream log) {
/*  67 */     String sql = 
/*  68 */       "SELECT\tDependency_System, Dependency_Job, B.Last_JobStatus, B.Last_TXDate \t\tFROM\tETL_Job_Dependency A,  ETL_JOB B \t\tWHERE\tA.ETL_System = '" + 
/*  70 */       sys + "' AND\tA.ETL_Job = '" + job + "' AND\tA.Enable = '1'  " + 
/*  71 */       "\t\t\tAND A.Dependency_System=B.ETL_System AND A.Dependency_Job = B.ETL_Job " + 
/*  72 */       "\t\t\tAND (B.Last_JobStatus <>'Done' OR B.Last_TXDate < CAST('" + txdate + "' AS DATE FORMAT 'YYYYMMDD')) ";
/*     */ 
/*  74 */     boolean ret = true;
/*  75 */     String dsys = null; String djob = null;
/*     */     try {
/*  77 */       Statement st = con.createStatement();
/*  78 */       ResultSet rs = st.executeQuery(sql);
/*  79 */       if (rs.next()) {
/*  80 */         dsys = rs.getString(1); djob = rs.getString(2);
/*  81 */         ret = false;
/*     */       }
/*  83 */       rs.close(); st.close(); } catch (SQLException e) {
/*  84 */       dberr = true;
/*  85 */     }if (!ret) {
/*  86 */       ETL.ShowPrefixSpace(log);
/*  87 */       log.println("Dependent job [" + dsys + "," + djob + "] does not finish yet, wait for next time!");
/*  88 */       if (ETL.IsJobAlreadyHasEvent(sys + "_" + job, "Dependent Job"))
/*  89 */         return ret;
/*  90 */       String eventDesc = "[" + sys + "," + job + "] has dependent job[" + dsys + "," + djob + "] did not finish yet!";
/*  91 */       ETL.InsertEventLog(con, "MAS", "L", eventDesc);
/*     */     }
/*  93 */     return ret;
/*     */   }
/*     */ 
/*     */   public static boolean doSchedule(Connection con, PrintStream log) {
/*  97 */     Calendar c = Calendar.getInstance();
/*  98 */     String tDate = String.format("%1$tY%1$tm%1$td", new Object[] { c });
/*  99 */     String tTime = String.format("%1$tH%1$tM", new Object[] { c });
/* 100 */     if (tTime.equals(lastRunTime)) return false;
/* 101 */     if (!ETL.isPrimaryServer) return false;
/* 102 */     lastRunTime = tTime;
/* 103 */     String sql = String.format(
/* 105 */       "SELECT\tA.ETL_Server, A.ETL_System, A.ETL_Job ,  calendar_date - OffsetDay txDate ,                             \n\t\t%1$s tDate, calendar_date,                                                                              \n\t\tCAST(((calendar_date(INTEGER))+19000000)*10000 + StartHour*100 + StartMin AS DEC(12,0)) trigerTime      \nFROM\tETL_Job A,  ETL_TimeTrigger  B,\tETL_Server C, Sys_Calendar.calendar D                                   \nWHERE\tTimeTrigger = 'Y'  AND\tEnable = '1'  AND\tJobType = 'D'                                               \n\tAND A.ETL_Server = C.ETL_Server                                                                                        \n\tAND A.ETL_System = B.ETL_System AND\tA.ETL_Job = B.ETL_Job                                                   \n\tAND lastRunDate<= calendar_date AND calendar_date<=(tDate-19000000)(DATE)                                 \n\tAND\t((lastRunDate(INTEGER))+19000000)*10000 + lastRunTime < trigerTime                                      \n\tAND tDate*10000 + %3$s >= trigerTime                                                                        \nUNION ALL                                                                                                      \nSELECT\tA.ETL_Server, A.ETL_System, A.ETL_Job ,  calendar_date - OffsetDay txDate ,                             \n\t\t%1$s tDate, calendar_date,                                                                              \n\t\tCAST(( (calendar_date(INTEGER))+19000000)*10000 + StartHour*100 + StartMin AS DEC(12,0)) trigerTime     \nFROM   ETL_Job A,  ETL_TimeTrigger  B,\tETL_Server C,                                                           \n\t   Sys_Calendar.calendar D, ETL_TimeTrigger_Weekly W                                                        \nWHERE\tTimeTrigger = 'Y' \tAND\tJobType = 'W'\tAND\tEnable = '1' AND A.ETL_Server=C.ETL_Server                  \n\tAND A.ETL_System = B.ETL_System AND\tA.ETL_Job = B.ETL_Job                                                   \n\tAND lastRunDate<= calendar_date AND calendar_date<=(tDate-19000000)(DATE)                                 \n\tAND\t((lastRunDate(INTEGER))+19000000)*10000 + lastRunTime < trigerTime                                      \n\tAND tDate*10000 + %3$s >= trigerTime                                                                        \n\tAND A.ETL_System = W.ETL_System AND\tA.ETL_Job = W.ETL_Job                                                   \n\tAND SUBSTR(timing, day_of_week, 1) = 'Y'                                                                    \nUNION ALL                                                                                                      \nSELECT\tA.ETL_Server, A.ETL_System, A.ETL_Job ,  calendar_date - OffsetDay txDate ,                             \n\t\t%1$s tDate, calendar_date,                                                                              \n\t\tCAST(( (calendar_date(INTEGER))+19000000)*10000 + StartHour*100 + StartMin AS DEC(12,0)) trigerTime     \nFROM   ETL_Job A,  ETL_TimeTrigger  B,\tETL_Server C,                                                           \n\t   Sys_Calendar.calendar D, ETL_TimeTrigger_Monthly M                                                       \nWHERE\tTimeTrigger = 'Y' \tAND\tJobType = 'M'\tAND\tEnable = '1' AND A.ETL_Server=C.ETL_Server                 \n\tAND A.ETL_System = B.ETL_System AND\tA.ETL_Job = B.ETL_Job                                                   \n\tAND lastRunDate<= calendar_date AND calendar_date<=(tDate-19000000)(DATE)                                 \n\tAND\t((lastRunDate(INTEGER))+19000000)*10000 + lastRunTime < trigerTime                                      \n\tAND tDate*10000 + %3$s >= trigerTime                                                                        \n\tAND A.ETL_System = M.ETL_System AND\tA.ETL_Job = M.ETL_Job                                                   \n\tAND (SUBSTR(timing, day_of_month, 1) = 'Y'  OR                                                              \n\t      EndOfMonth = 'Y' AND EXTRACT( DAY FROM calendar_date + 1) = 1 )                                       \nUNION ALL                                                                                                      \nSELECT\tA.ETL_Server, A.ETL_System, A.ETL_Job ,  calendar_date - OffsetDay txDate ,                           \n\t\t%1$s tDate, ((YearNum*10000+MonthNum*100 + DayNum) - 19000000) (DATE) calendar_date,                  \n\t\tCAST(( (calendar_date(INTEGER))+19000000)*10000 + StartHour*100 + StartMin AS DEC(12,0)) trigerTime   \nFROM   ETL_Job A,  ETL_TimeTrigger  B,\tETL_Server C,                                                         \n\t   ETL_TimeTrigger_Calendar D                                                                             \nWHERE\tTimeTrigger = 'Y' \tAND\tJobType = '9'\tAND\tEnable = '1' AND A.ETL_Server=C.ETL_Server                \n\tAND A.ETL_System = B.ETL_System AND\tA.ETL_Job = B.ETL_Job                                                 \n\tAND lastRunDate<= calendar_date AND calendar_date<=(tDate-19000000)(DATE)                                 \n\tAND\t((lastRunDate(INTEGER))+19000000)*10000 + lastRunTime < trigerTime                                      \n\tAND tDate*10000 + %3$s >= trigerTime                                                                        \n\tAND A.ETL_System = D.ETL_System AND\tA.ETL_Job = D.ETL_Job                                                   \n", new Object[] { 
/* 160 */       tDate, ETL.Auto_server, tTime });
/*     */     try {
/* 162 */       Statement st = con.createStatement();
/* 163 */       ResultSet rs = st.executeQuery(sql);
/* 164 */       int count = 0;
/* 165 */       Vector jobList = new Vector();
/* 166 */       while (rs.next()) {
/* 167 */         ++count;
/* 168 */         jobList.add(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\t" + rs.getString(4));
/*     */       }
/* 170 */       rs.close(); st.close();
/* 171 */       if (count > 0) {
/* 172 */         ETL.ShowTime(log); log.println("Timely Triggered Job");
/*     */       }
/* 174 */       sql = "INSERT INTO ETL_Job_Queue(ETL_Server, SeqID, ETL_System, ETL_Job, TXDate, RequestTime)VALUES(?, 0, ?, ?, ? , '" + 
/* 175 */         ETL.GetDateTime() + "')";
/* 176 */       PreparedStatement pst = con.prepareStatement(sql);
/* 177 */       for (int i = 0; i < count; ++i) {
/* 178 */         String[] jobW = ((String)jobList.elementAt(i)).split("\t");
/* 179 */         pst.setString(1, jobW[0]);
/* 180 */         pst.setString(2, jobW[1]);
/* 181 */         pst.setString(3, jobW[2]);
/* 182 */         pst.setString(4, jobW[3]);
/* 183 */         pst.execute();
/* 184 */         ETL.ShowPrefixSpace(log);
/* 185 */         log.println("Generate job [" + jobW[1] + "," + jobW[2] + "] into job queue for '" + jobW[3] + "'");
/*     */       }
/* 187 */       pst.close();
/* 188 */       if (count > 0) {
/* 189 */         sql = "UPDATE ETL_Server SET lastRunDate=CAST('" + tDate + "' AS DATE FORMAT 'YYYYMMDD'), lastRunTime=" + tTime;
/* 190 */         st.execute(sql); st.close();
/* 191 */         ETL.ShowPrefixSpace(log);
/* 192 */         log.println("Update Automation lastRunDate and time to '" + tDate + " " + tTime + "'.");
/*     */       }
/*     */     } catch (SQLException e) {
/* 194 */       e.printStackTrace(); dberr = true;
/* 195 */     }return dberr;
/*     */   }
/*     */ 
/*     */   public static boolean CheckJobQueue(Connection con, PrintStream log) {
/* 199 */     dberr = false;
/*     */ 
/* 201 */     String sql = "SELECT A.ETL_System, A.ETL_Job, (TXDate (INT))+19000000 , B.SOURCE FROM etl_job_queue A, ETL_Job_Source B WHERE ETL_Server = '" + 
/* 203 */       ETL.Auto_server + "' AND A.ETL_System = B.ETL_System " + 
/* 204 */       "AND A.ETL_Job = B.ETL_Job \tORDER BY 3 ";
/* 205 */     String sql2 = "DELETE FROM ETL_Job_Queue WHERE ETL_Server='" + ETL.Auto_server + 
/* 206 */       "' AND ETL_System = ? AND ETL_Job = ? AND TXDate = ?";
/*     */ 
/* 208 */     int count = 0;
/*     */     try {
/* 210 */       PreparedStatement pst = con.prepareStatement(sql2);
/*     */ 
/* 212 */       Statement st = con.createStatement();
/* 213 */       ResultSet rs = st.executeQuery(sql);
/* 214 */       while (rs.next()) {
/* 215 */         if (count == 0) {
/* 216 */           ETL.ShowTime(log); log.println("Generate control file from ETL_Job_queue");
/*     */         }
/* 218 */         ++count;
/* 219 */         String sys = rs.getString(1); String job = rs.getString(2); String txDate = rs.getString(3); String source = rs.getString(4);
/* 220 */         ETL.ShowPrefixSpace(log); log.println("System:[" + sys + "], Job:[" + job + "], TxDate:[" + txDate + "]");
/* 221 */         ETL.ShowPrefixSpace(log); log.println("Generate control file dir." + source + txDate);
/* 222 */         String tx_date = txDate.substring(0, 4) + "-" + txDate.substring(4, 6) + 
/* 223 */           "-" + txDate.substring(6, 8);
/*     */         try {
/* 225 */           String fileName = ETL.Auto_home + "/DATA/receive/dir." + source + txDate;
/* 226 */           FileOutputStream cf = new FileOutputStream(fileName);
/* 227 */           cf.close();
/* 228 */           RCV.IsSizeStable(fileName);
/* 229 */           pst.setString(1, sys);
/* 230 */           pst.setString(2, job);
/* 231 */           pst.setString(3, tx_date);
/* 232 */           pst.execute();
/*     */         }
/*     */         catch (FileNotFoundException e) {
/* 235 */           e.printStackTrace(log);
/*     */         } catch (IOException e) {
/* 237 */           e.printStackTrace(log);
/*     */         }
/*     */       }
/* 240 */       rs.close();
/* 241 */       st.close(); pst.close(); } catch (SQLException e) {
/* 242 */       e.printStackTrace(log); dberr = true;
/* 243 */     }return dberr;
/*     */   }
/*     */   public static boolean CheckJobQueue1(Connection con, PrintStream log) {
/* 246 */     dberr = false;
/*     */ 
/* 248 */     String sql = "SELECT A.ETL_System, A.ETL_Job, (TXDate (INT))+19000000 , B.SOURCE FROM etl_job_queue A, ETL_Job_Source B WHERE ETL_Server = '" + 
/* 250 */       ETL.Auto_server + "' AND A.ETL_System = B.ETL_System " + 
/* 251 */       "AND A.ETL_Job = B.ETL_Job \tORDER BY 3 ";
/*     */ 
/* 253 */     String triggeredJobs = "";
/* 254 */     int count = 0;
/*     */     try {
/* 256 */       Statement st = con.createStatement();
/* 257 */       ResultSet rs = st.executeQuery(sql);
/* 258 */       while (rs.next()) {
/* 259 */         if (count == 0) {
/* 260 */           ETL.ShowTime(log); log.println("Generate control file from ETL_Job_queue");
/*     */         }
/* 262 */         ++count;
/* 263 */         String sys = rs.getString(1); String job = rs.getString(2); String txDate = rs.getString(3); String source = rs.getString(4);
/* 264 */         ETL.ShowPrefixSpace(log); log.println("System:[" + sys + "], Job:[" + job + "], TxDate:[" + txDate + "]");
/* 265 */         ETL.ShowPrefixSpace(log); log.println("Generate control file dir." + source + txDate);
/* 266 */         triggeredJobs = triggeredJobs + sys + "/" + job + "/" + txDate + "\t";
/*     */         try {
/* 268 */           String fileName = ETL.Auto_home + "/DATA/receive/dir." + source + txDate;
/* 269 */           FileOutputStream cf = new FileOutputStream(fileName);
/* 270 */           cf.close();
/* 271 */           RCV.IsSizeStable(fileName);
/*     */         } catch (FileNotFoundException e) {
/* 273 */           e.printStackTrace(log);
/*     */         } catch (IOException e) {
/* 275 */           e.printStackTrace(log);
/*     */         }
/*     */       }
/* 278 */       rs.close();
/* 279 */       if (count > 0) {
/* 280 */         String[] w = triggeredJobs.split("\t");
/* 281 */         String sqlText = "DELETE FROM ETL_Job_Queue WHERE ETL_Server='" + ETL.Auto_server + 
/* 282 */           "' AND ETL_System = ? AND ETL_Job = ? AND TXDate = ?";
/* 283 */         PreparedStatement pst = con.prepareStatement(sqlText);
/* 284 */         for (int k = 0; k < count; ++k) {
/* 285 */           String[] sj = w[k].split("/");
/* 286 */           String tx_date = sj[2].substring(0, 4) + "-" + sj[2].substring(4, 6) + 
/* 287 */             "-" + sj[2].substring(6, 8);
/* 288 */           pst.setString(1, sj[0]);
/* 289 */           pst.setString(2, sj[1]);
/* 290 */           pst.setString(3, tx_date);
/* 291 */           pst.execute();
/*     */         }
/* 293 */         pst.close();
/*     */       }
/* 295 */       st.close(); } catch (SQLException e) {
/* 296 */       e.printStackTrace(log); dberr = true;
/* 297 */     }return dberr;
/*     */   }
/*     */ 
/*     */   public static boolean CheckPendingJob(Connection conIn, PrintStream log) {
/* 301 */     con = conIn;
/* 302 */     Vector jobs = getPendingJob();
/* 303 */     for (int i = 0; i < jobs.size(); ++i) {
/* 304 */       if (dberr) return false;
/* 305 */       if (ETL.Stop_Flag) break;
/* 306 */       String[] w = ((String)jobs.elementAt(i)).split("\t");
/* 307 */       String sys = w[0]; String job = w[1]; String txdate = w[2]; String jobType = w[3];
/* 308 */       File ctlF = getControlFile(con, (String)jobs.elementAt(i), ETL.Auto_home + "/DATA/queue");
/* 309 */       if (ctlF == null) {
/* 310 */         ETL.ShowTime(log); log.println("The Pending Job [" + sys + "," + job + "] has not JOB-Source");
/* 311 */         if (ETL.IsJobAlreadyHasEvent(sys + "_" + job, "has not JOB-Source"))
/*     */           break;
/* 313 */         String eventDesc = "[" + sys + "," + job + "] need define JOB-Source !";
/* 314 */         ETL.InsertEventLog(con, "MAS", "M", eventDesc);
/*     */       }
/*     */       else {
/* 317 */         ETL.ShowTime(log); log.println("Processing control file '" + ctlF.getName() + "'");
/* 318 */         ETL.ShowPrefixSpace(log); log.println("System:[" + sys + "], Job:[" + job + "], TxDate:[" + txdate + "]");
/* 319 */         if (!checkJobDependency(sys, job, txdate, log)) {
/*     */           continue;
/*     */         }
/* 322 */         if ((!jobType.equals("V")) && (JobRunner.RunningCount.get() >= ETL.AutoMaxJobCount)) {
/* 323 */           ETL.ShowTime(log); log.print("Current running jobs reached the limitation, wait for next time!\n");
/* 324 */           if (ETL.IsJobAlreadyHasEvent(sys + "_" + job, "Max Job Count Job"))
/*     */             break;
/* 326 */           String eventDesc = "[" + sys + "," + job + "] Current running jobs has reached the limitation, wait for next time!";
/* 327 */           ETL.InsertEventLog(con, "MAS", "M", eventDesc);
/* 328 */           break;
/*     */         }
/*     */ 
/* 331 */         if ((!jobType.equals("V")) && (getStepNumber(sys, job) < 1)) {
/* 332 */           ETL.ShowPrefixSpace(log); log.print("No steps for the job, wait for next time!\n");
/* 333 */           if (ETL.IsJobAlreadyHasEvent(sys + "_" + job, "No Job Step")) {
/*     */             continue;
/*     */           }
/* 336 */           String eventDesc = "[" + sys + "," + job + "] No steps for the job, wait for next time!";
/* 337 */           ETL.InsertEventLog(con, "MAS", "H", eventDesc);
/*     */         }
/*     */         else
/*     */         {
/* 343 */           ETL.ShowPrefixSpace(log); log.print("Check Dependant Job Running\n");
/* 344 */           if (isDependentJobRunning(con, sys, job)) {
/* 345 */             ETL.ShowPrefixSpace(log); log.print("There is dependant job running, wait for next time!\n");
/*     */ 
/* 347 */             if (ETL.IsJobAlreadyHasEvent(sys + "_" + job, "Dependant Job Running")) {
/*     */               continue;
/*     */             }
/* 350 */             String eventDesc = "[" + sys + "," + job + "]has dependant job running, wait for next time!";
/* 351 */             ETL.InsertEventLog(con, "MAS", "L", eventDesc);
/*     */           }
/*     */           else
/*     */           {
/* 356 */             ETL.ShowPrefixSpace(log); log.print("Check Group Head Job Running\n");
/* 357 */             if (isHeadJobRunning(con, sys, job)) {
/* 358 */               ETL.ShowPrefixSpace(log); log.print("There is group head job running, wait for next time!\n");
/*     */ 
/* 360 */               if (ETL.IsJobAlreadyHasEvent(sys + "_" + job, "Head Job Running")) {
/*     */                 continue;
/*     */               }
/* 363 */               String eventDesc = "[" + sys + "," + job + "] has group head job running, wait for next time!";
/* 364 */               ETL.InsertEventLog(con, "MAS", "L", eventDesc);
/*     */             }
/*     */             else
/*     */             {
/* 369 */               ETL.ShowPrefixSpace(log); log.print("Check Group Child Job Running\n");
/* 370 */               if (isChildJobRunning(con, sys, job)) {
/* 371 */                 ETL.ShowPrefixSpace(log); log.print("There is group child job running, wait for next time!\n");
/*     */ 
/* 373 */                 if (ETL.IsJobAlreadyHasEvent(sys + "_" + job, "Child Job Running")) {
/*     */                   continue;
/*     */                 }
/* 376 */                 String eventDesc = "[" + sys + "," + job + "] has group child job running, wait for next time!";
/* 377 */                 ETL.InsertEventLog(con, "MAS", "L", eventDesc);
/*     */               }
/*     */               else
/*     */               {
/* 381 */                 ETL.ShowPrefixSpace(log); log.print("Submit the job[" + sys + "," + job + "] control file=" + 
/* 382 */                   ctlF.getName() + "\n");
/* 383 */                 ContentControl ct = new ContentControl(ctlF.getPath());
/* 384 */                 ct.MoveTo(ETL.Auto_home + "/DATA/process");
/* 385 */                 ct.updateFileLocation(con, ETL.Auto_home + "/DATA/process", sys, job);
/*     */ 
/* 387 */                 JobRunner.submitFisrtStep(con, sys, job, ctlF.getName());
/*     */               }
/*     */             }
/*     */           }
/*     */         }
/*     */       }
/*     */     }
/* 389 */     return dberr;
/*     */   }
/*     */ 
/*     */   private static boolean isChildJobRunning(Connection con, String sys, String job)
/*     */   {
/* 395 */     String sql = String.format(
/* 396 */       "\t\tSELECT\t C.Last_JobStatus \t\tFROM\tETL_Job_GroupChild A, ETL_Job_Group B,  ETL_JOB C \t\tWHERE\tB.ETL_System = '%1$s' \t\t\tAND\tB.ETL_Job = '%2$s' \t\t\tAND\tA.Enable = '1' \t\t\tAND A.GroupName=B.GroupName AND A.ETL_System  = C.ETL_System AND A.ETL_Job = C.ETL_Job \t\t\tAND C.Last_JobStatus ='Running' ", new Object[] { 
/* 402 */       sys, job });
/* 403 */     boolean ret = false;
/*     */     try {
/* 405 */       Statement st = con.createStatement();
/* 406 */       ResultSet rs = st.executeQuery(sql);
/* 407 */       if (rs.next()) {
/* 408 */         ret = true;
/*     */       }
/* 410 */       rs.close(); st.close(); } catch (SQLException e) {
/* 411 */       dberr = true;
/* 412 */     }return ret;
/*     */   }
/*     */   private static boolean isHeadJobRunning(Connection con, String sys, String job) {
/* 415 */     String sql = String.format(
/* 416 */       "\t\tSELECT\t C.Last_JobStatus \t\tFROM\tETL_Job_GroupChild A, ETL_Job_Group B,  ETL_JOB C \t\tWHERE\tA.ETL_System = '%1$s' \t\t\tAND\tA.ETL_Job = '%2$s' \t\t\tAND\tA.Enable = '1' \t\t\tAND A.GroupName=B.GroupName AND B.ETL_System  = C.ETL_System AND B.ETL_Job = C.ETL_Job \t\t\tAND C.Last_JobStatus ='Running' ", new Object[] { 
/* 422 */       sys, job });
/* 423 */     boolean ret = false;
/*     */     try {
/* 425 */       Statement st = con.createStatement();
/* 426 */       ResultSet rs = st.executeQuery(sql);
/* 427 */       if (rs.next()) {
/* 428 */         ret = true;
/*     */       }
/* 430 */       rs.close(); st.close(); } catch (SQLException e) {
/* 431 */       dberr = true;
/* 432 */     }return ret;
/*     */   }
/*     */   private static boolean isDependentJobRunning(Connection con2, String sys, String job) {
/* 435 */     String sql = String.format(
/* 436 */       "\t\tSELECT\t Dependency_System, Dependency_Job \t\tFROM\tETL_Job_Dependency A,  ETL_JOB B \t\tWHERE\tA.ETL_System = '%1$s' \t\t\tAND\tA.ETL_Job = '%2$s'\t\t\t\tAND\tA.Enable = '1'  \t\t\tAND A.Dependency_System=B.ETL_System AND A.Dependency_Job = B.ETL_Job \t\t\tAND B.Last_JobStatus ='Running'  \t\tUNION ALL \t\tSELECT\tA.ETL_System , A.ETL_Job \t\tFROM\tETL_Job_Dependency A,  ETL_JOB B \t\tWHERE\tA.Dependency_System = '%1$s' \t\t\tAND\tA.Dependency_Job = '%2$s' \t\t\tAND\tA.Enable = '1' \t\t\tAND A.ETL_System=B.ETL_System AND A.ETL_Job = B.ETL_Job \t\t\tAND B.Last_JobStatus ='Running' ", new Object[] { 
/* 450 */       sys, job });
/* 451 */     boolean ret = false;
/*     */     try {
/* 453 */       Statement st = con.createStatement();
/* 454 */       ResultSet rs = st.executeQuery(sql);
/* 455 */       if (rs.next()) {
/* 456 */         ret = true;
/*     */       }
/* 458 */       st.close(); st.close(); } catch (SQLException e) {
/* 459 */       dberr = true;
/* 460 */     }return ret;
/*     */   }
/*     */   private static int getStepNumber(String sys, String job) {
/* 463 */     String sql = "SEL COUNT(*) FROM ETL_Job_Step WHERE ETL_System = '" + sys + "' AND ETL_Job = '" + 
/* 464 */       job + "' AND Enable='1'";
/* 465 */     int count = 0;
/*     */     try {
/* 467 */       Statement st = con.createStatement();
/* 468 */       ResultSet rs = st.executeQuery(sql);
/* 469 */       if (rs.next()) {
/* 470 */         count = rs.getInt(1);
/*     */       }
/* 472 */       rs.close(); st.close(); } catch (SQLException e) {
/* 473 */       dberr = true;
/* 474 */     }return count;
/*     */   }
/*     */   public static void ResetRunningJob(Connection con, PrintStream log) {
/* 477 */     String sql = "Update ETL_Job SET Last_JobStatus='Failed', JobSessionID = JobSessionId+1  WHERE Last_JobStatus = 'Running' AND ETL_Server='" + 
/* 478 */       ETL.Auto_server + "'";
/*     */     try {
/* 480 */       Statement st = con.createStatement();
/* 481 */       int rc = st.executeUpdate(sql);
/* 482 */       st.close();
/* 483 */       if (rc > 0) {
/* 484 */         ETL.ShowPrefixSpace(log); log.print("Warning Total " + rc + 
/* 485 */           " Jobs is abend because of DWAuto Restart.\n");
/*     */       }
/*     */     } catch (SQLException e) {
/* 487 */       dberr = true;
/*     */     }
/*     */   }
/*     */ }

/* Location:           D:\TD_CMP\Automation2.7\DWAuto_server\DWAuto.jar
 * Qualified Name:     etl.Master
 * JD-Core Version:    0.5.4
 */