/*     */ package etl;
/*     */ 
/*     */ import java.io.File;
/*     */ import java.io.FileOutputStream;
/*     */ import java.io.PrintStream;
/*     */ import java.sql.Connection;
/*     */ import java.sql.ResultSet;
/*     */ import java.sql.SQLException;
/*     */ import java.sql.Statement;
/*     */ import java.util.Arrays;
/*     */ import java.util.Calendar;
/*     */ import java.util.Enumeration;
/*     */ import java.util.Hashtable;
/*     */ 
/*     */ public class RCV
/*     */ {
/*   8 */   private static Connection con = null;
/*   9 */   private static String rcvDir = "";
/*  10 */   public static boolean dberr = false;
/*     */   private static PrintStream log;
/*  12 */   private static Hashtable<String, Long> StableCheckSize = new Hashtable();
/*     */ 
/*     */   private static void RefreshSizeStable()
/*     */   {
/*  17 */     for (Enumeration e = StableCheckSize.keys(); e.hasMoreElements(); ) {
/*  18 */       String key = (String)e.nextElement();
/*  19 */       File fremove = new File(key);
/*  20 */       if (fremove.exists())
/*     */         continue;
/*  22 */       StableCheckSize.remove(key);
/*     */     }
/*     */   }
/*     */ 
/*     */   public static boolean IsSizeStable(String fileName) {
/*  27 */     File f = new File(fileName); long len = f.length();
/*  28 */     Long oldLen = (Long)StableCheckSize.get(fileName);
/*  29 */     if (oldLen == null) {
/*  30 */       StableCheckSize.put(fileName, Long.valueOf(len));
/*  31 */       return false;
/*     */     }
/*  33 */     if (oldLen.longValue() < len) {
/*  34 */       StableCheckSize.put(fileName, Long.valueOf(len));
/*  35 */       return false;
/*     */     }
/*     */ 
/*  38 */     return true;
/*     */   }
/*     */ 
/*     */   private static String GetSrc(String controlFile)
/*     */   {
/*  43 */     return controlFile.substring(4, controlFile.length() - 8);
/*     */   }
/*     */ 
/*     */   private static String GetTXDate(String controlFile) {
/*  47 */     return controlFile.substring(controlFile.length() - 8);
/*     */   }
/*     */ 
/*     */   public static void CheckReceiveDir(Connection conIn, PrintStream logIn)
/*     */   {
/*  52 */     ETL.GetToday();
/*  53 */     con = conIn;
/*  54 */     rcvDir = ETL.Auto_home + "/DATA/receive";
/*  55 */     dberr = false; log = logIn;
/*     */ 
/*  57 */     ETL.ShowTime(log); log.println("Checking receiving directory '" + rcvDir + "...");
/*  58 */     File f = new File(rcvDir);
/*  59 */     File[] fns = f.listFiles(new ControlFilter(1));
/*  60 */     if (fns == null) {
/*  61 */       log.println("The Path is not exist.");
/*  62 */       return;
/*     */     }
/*  64 */     RefreshSizeStable();
/*  65 */     String[] controlList = new String[fns.length];
/*  66 */     int lstLen = 0; int j = 0;
/*  67 */     for (int i = 0; i < fns.length; ++i) {
/*  68 */       if (!IsSizeStable(fns[i].getPath()))
/*     */         continue;
/*  70 */       String source = GetSrc(fns[i].getName());
/*  71 */       for (j = 0; j < lstLen; ++j)
/*  72 */         if (source.equals(GetSrc(controlList[j])))
/*     */           break;
/*  74 */       if (j == lstLen) {
/*  75 */         controlList[(lstLen++)] = fns[i].getName();
/*     */       } else {
/*  77 */         String tx1 = GetTXDate(controlList[j]);
/*  78 */         if (GetTXDate(fns[i].getName()).compareTo(tx1) < 0) {
/*  79 */           controlList[j] = fns[i].getName();
/*     */         }
/*     */       }
/*     */     }
/*     */ 
/*  84 */     if (lstLen == 0)
/*  85 */       return;
/*  86 */     Arrays.sort(controlList, 0, lstLen, new ControlFileCompare());
/*     */ 
/*  88 */     for (int i = 0; i < lstLen; ++i) {
/*  89 */       if (ETL.Stop_Flag) return;
/*  90 */       if (ProcessControlFile(controlList[i], log)) return;
/*     */     }
/*     */   }
/*     */ 
/*     */   private static boolean CheckRelatedJobStatus(JobInfo job, String txDate, PrintStream out) {
/*  95 */     boolean ret = true;
/*  96 */     ETL.ShowPrefixSpace(out); out.println("Check related job status...");
/*  97 */     String relCheck = 
/*  98 */       "SELECT  B.ETL_System, B.ETL_Job, A.CheckMode, Last_JobStatus, Last_TXDate, JobType, CheckCalendar,  cast('%3$s' as date format 'YYYYMMDD') - Last_TXDate  dayDiff          \nFROM ETL_RelatedJob A, ETL_JOB B                                       \nWHERE A.etl_system = '%1$s' AND A.etl_job='%2$s'                       \n  AND A.RelatedSystem = B.ETL_System  AND A.RelatedJob = B.ETL_Job     \n";
/*     */ 
/* 103 */     String sql = String.format(relCheck, new Object[] { job.sysName, job.jobName, txDate });
/*     */     try {
/* 105 */       Statement st = con.createStatement();
/* 106 */       ResultSet rs = st.executeQuery(sql);
/* 107 */       String relCalCheckJobs = "";
/* 108 */       while (rs.next())
/*     */       {
/* 110 */         String sys = rs.getString("ETL_System"); String reljob = rs.getString("ETL_Job"); String chkMode = rs.getString("CheckMode");
/* 111 */         String status = rs.getString("Last_JobStatus");
/* 112 */         String lastTxdate = rs.getString("Last_TXDate"); lastTxdate = lastTxdate.replaceAll("-", "");
/* 113 */         String jobType = rs.getString("JobType"); String checkCal = rs.getString("CheckCalendar");
/* 114 */         int dayDiff = rs.getInt("dayDiff"); if (rs.wasNull()) dayDiff = 0;
/*     */ 
/* 116 */         if (!status.equalsIgnoreCase("Done")) {
/* 117 */           ret = false;
/* 118 */           ETL.ShowPrefixSpace(out);
/* 119 */           out.println("Related job " + sys + "," + reljob + " is not Done. Current is " + status);
/* 120 */         } else if (checkCal.equalsIgnoreCase("Y")) {
/* 121 */           relCalCheckJobs = relCalCheckJobs + "\t" + sys + "," + reljob + "," + chkMode + "," + lastTxdate;
/*     */         }
/* 123 */         else if (jobType.equalsIgnoreCase("D")) {
/* 124 */           ETL.ShowPrefixSpace(out);
/* 125 */           out.println("Related job [" + sys + "," + reljob + "] is daily job.");
/* 126 */           if (dayDiff <= 1) {
/* 127 */             out.println("Related daily job date offset is in range. txDate=" + lastTxdate);
/*     */           } else {
/* 129 */             ret = false;
/* 130 */             out.println("Related daily job date offset is over range. txDate=" + lastTxdate);
/*     */           }
/* 132 */         } else if (jobType.equalsIgnoreCase("W")) {
/* 133 */           ETL.ShowPrefixSpace(out);
/* 134 */           out.println("Related job [" + sys + "," + reljob + "] is weekly job.");
/* 135 */           if (dayDiff <= 7) {
/* 136 */             out.println("Related daily job date offset is in range. txDate=" + lastTxdate);
/*     */           } else {
/* 138 */             ret = false;
/* 139 */             out.println("Related daily job date offset is over range. txDate=" + lastTxdate);
/*     */           }
/* 141 */         } else if (jobType.equalsIgnoreCase("M")) {
/* 142 */           ETL.ShowPrefixSpace(out);
/* 143 */           out.println("Related job [" + sys + "," + reljob + "] is Monthly job.");
/* 144 */           int last = Integer.parseInt(lastTxdate.substring(0, 6));
/* 145 */           int txd = Integer.parseInt(txDate.substring(0, 6));
/* 146 */           int diffMon = txd / 100 * 12 + txd % 100 - last / 100 * 12 - last % 100;
/* 147 */           if (diffMon <= 1) {
/* 148 */             out.println("Related Monthly job date offset is in range.");
/*     */           } else {
/* 150 */             ret = false;
/* 151 */             out.println("Related Monthly job date offset is over range.");
/*     */           }
/*     */         }
/*     */       }
/*     */ 
/* 156 */       rs.close();
/* 157 */       if ((ret) && (relCalCheckJobs.length() > 0)) {
/* 158 */         ETL.ShowPrefixSpace(out); out.println("Check related job by data calendar");
/* 159 */         String[] relJobs = relCalCheckJobs.split("\t");
/* 160 */         String sqlCal = "SELECT CalendarYear*10000 + CalendarMonth*100 + CalendarDay dt FROM  DataCalendar WHERE ETL_system = '%1$s' AND ETL_job='%2$s'   AND dt %4$s %3$s ORDER BY dt DESC ";
/*     */ 
/* 163 */         for (int k = 1; k < relJobs.length; ++k) {
/* 164 */           String[] rel = relJobs[k].split(",");
/*     */ 
/* 166 */           String cmp = (rel[2].equals("1")) ? "<" : "<=";
/* 167 */           ETL.ShowPrefixSpace(out);
/* 168 */           out.format("Get prior calendar date, %1$s, %2$s, %3$s, %4$s\n", new Object[] { rel[0], rel[1], rel[2], txDate });
/* 169 */           sql = String.format(sqlCal, new Object[] { rel[0], rel[1], txDate, cmp });
/* 170 */           rs = st.executeQuery(sql);
/* 171 */           String priorDate = "0000-00-00";
/* 172 */           if (rs.next()) priorDate = rs.getString(1);
/* 173 */           ETL.ShowPrefixSpace(out); out.println("Related job date should be " + priorDate);
/* 174 */           rs.close();
/* 175 */           if ((priorDate.equals(rel[3])) || (priorDate.equals("0000-00-00")))
/*     */             continue;
/* 177 */           ETL.ShowPrefixSpace(out); out.println("    But date is " + rel[3]);
/* 178 */           ret = false;
/*     */         }
/*     */       }
/*     */ 
/* 182 */       st.close(); } catch (SQLException ex) {
/* 183 */       dberr = true;
/* 184 */     }ETL.ShowPrefixSpace(out);
/* 185 */     if (ret)
/* 186 */       out.println("Check related job status ok.");
/*     */     else
/* 188 */       out.println("Check related job status fail.");
/* 189 */     return ret;
/*     */   }
/*     */ 
/*     */   private static boolean ProcessControlFile(String controlFile, PrintStream log)
/*     */   {
/* 194 */     ETL.ShowTime(log); log.println("Processing control file '" + controlFile + "'...");
/* 195 */     String jobSource = GetSrc(controlFile);
/* 196 */     String txDate = GetTXDate(controlFile);
/*     */ 
/* 198 */     JobInfo job = new JobInfo(con, jobSource);
/* 199 */     ETL.ShowPrefixSpace(log);
/* 200 */     log.println("System='" + job.sysName + "', Job='" + job.jobName + "', ConvHead='" + job.convHead + "' TXDATE=" + txDate);
/* 201 */     if (dberr) return true;
/* 202 */     ContentControl cont = new ContentControl(rcvDir, controlFile);
/* 203 */     if ((job.sysName == null) || (!ETL.isOKDate(txDate))) {
/* 204 */       ETL.ShowTime(log); log.println("Unknown control file '" + controlFile + "'");
/* 205 */       String desc = "Unknown control file " + controlFile;
/* 206 */       ETL.InsertEventLog(con, "RCV", "L", desc);
/* 207 */       cont.MoveToday(ETL.Auto_home + "/DATA/fail/unknown");
/* 208 */       return false;
/*     */     }
/* 210 */     if (UpdateSourceLastCount(jobSource)) return true;
/*     */ 
/* 213 */     if (!cont.CheckDataFileSize()) {
/* 214 */       cont.MoveToday(ETL.Auto_home + "/DATA/fail/corrupt/" + job.sysName);
/* 215 */       String eventDesc = "[" + job.sysName + "," + job.jobName + " has corrupt data in control file " + controlFile;
/* 216 */       ETL.InsertEventLog(con, "RCV", "H", eventDesc);
/*     */ 
/* 218 */       ETL.ShowTime(log); log.println("Generate data file corrupt message...");
/* 219 */       ETL.WriteMessageNotification(job.sysName, job.jobName, txDate, 
/* 220 */         "Receiving", 
/* 221 */         "Job [" + job.sysName + "," + job.jobName + "] has corrupt data in control file " + controlFile, 
/* 222 */         "");
/* 223 */       return false;
/*     */     }
/*     */ 
/* 226 */     if (!job.enable.equals("1")) {
/* 227 */       ETL.ShowPrefixSpace(log); log.println("WARNING - The job is not enabled, we will process this file next time.");
/* 228 */       return false;
/*     */     }
/*     */ 
/* 231 */     if ((((!job.checkLastStatus.equalsIgnoreCase("N")) || (!job.jobStatus.equalsIgnoreCase("Failed")))) && 
/* 232 */       (!job.jobStatus.equalsIgnoreCase("Ready")) && (!job.jobStatus.equalsIgnoreCase("Done"))) {
/* 233 */       ETL.ShowPrefixSpace(log);
/* 234 */       log.println("WARNING - The job is in " + job.jobStatus + ", we will process this file next time.");
/* 235 */       if (ETL.IsJobAlreadyHasEvent(job.sysName + "_" + job.jobName, "Status Mismatch"))
/* 236 */         return false;
/* 237 */       String eventDesc = "[" + job.sysName + ", [" + job.jobName + "] still in <" + job.jobStatus + 
/* 238 */         " but has received another file " + controlFile;
/* 239 */       ETL.InsertEventLog(con, "RCV", "M", eventDesc);
/* 240 */       return false;
/*     */     }
/* 242 */     ETL.ShowPrefixSpace(log); log.println("Check job frequency " + job.sysName + ", " + job.jobName);
/* 243 */     if (!CheckJobFrequency(job.frequency, txDate)) {
/* 244 */       cont.MoveToday(ETL.Auto_home + "/DATA/fail/bypass/" + job.sysName);
/* 245 */       String eventDesc = "[" + job.sysName + "," + job.jobName + "] has received a control file " + controlFile + " did not match the frequency";
/* 246 */       ETL.InsertEventLog(con, "RCV", "L", eventDesc);
/*     */ 
/* 248 */       ETL.RemoveJobEventRecord(job.sysName + "_" + job.jobName);
/*     */ 
/* 250 */       ETL.ShowTime(log); log.println("Generate data file frequency did not match message...");
/* 251 */       ETL.WriteMessageNotification(job.sysName, job.jobName, txDate, 
/* 252 */         "Receiving", 
/* 253 */         String.format("Job [%1$s,%2$s] has received a control file %3$s did not match the frequency", new Object[] { 
/* 254 */         job.sysName, job.jobName, controlFile }), 
/* 255 */         "");
/* 256 */       return false;
/*     */     }
/* 258 */     if (job.checkCalendar.equalsIgnoreCase("Y")) {
/* 259 */       ETL.ShowPrefixSpace(log); log.println("Check data calendar " + job.sysName + ", " + job.jobName + " TXDATE=" + txDate);
/* 260 */       if (!CheckDataCalendar(job, txDate)) {
/* 261 */         ETL.ShowPrefixSpace(log); log.println("Data-calendar mismatch.");
/* 262 */         cont.MoveToday(ETL.Auto_home + "/DATA/fail/bypass/" + job.sysName);
/* 263 */         ETL.RemoveJobEventRecord(job.sysName + "_" + job.jobName);
/* 264 */         String eventDesc = "[" + job.sysName + "," + job.jobName + "] has received a control file " + controlFile + " did not match the data-calendar";
/* 265 */         ETL.InsertEventLog(con, "RCV", "L", eventDesc);
/*     */ 
/* 267 */         ETL.ShowTime(log); log.println("Generate data file did not match data-calendar message...");
/* 268 */         ETL.WriteMessageNotification(job.sysName, job.jobName, txDate, 
/* 269 */           "Receiving", 
/* 270 */           String.format("Job [%1$s,%2$s] has received a control file %3$s did not match the data-calendar", new Object[] { 
/* 271 */           job.sysName, job.jobName, controlFile }), 
/* 272 */           "");
/* 273 */         return false;
/*     */       }
/* 275 */       ETL.ShowPrefixSpace(log); log.println("Check data calendar OK.");
/*     */     }
/* 277 */     if (dberr) return true;
/*     */ 
/* 279 */     if (!CheckRelatedJobStatus(job, txDate, log)) {
/* 280 */       ETL.ShowTime(log); log.println("Check job related job failed!");
/* 281 */       if (ETL.IsJobAlreadyHasEvent(job.sysName + "_" + job.jobName, "Related Job"))
/* 282 */         return false;
/* 283 */       String eventDesc = "[" + job.sysName + "," + job.jobName + "] has related job did not finish yet, wait to next time";
/* 284 */       ETL.InsertEventLog(con, "RCV", "L", eventDesc);
/* 285 */       return false;
/*     */     }
/*     */ 
/* 288 */     if (txDate.compareTo(job.txDate) < 0) {
/* 289 */       cont.MoveToday(ETL.Auto_home + "/DATA/fail/bypass/" + job.sysName);
/* 290 */       ETL.ShowPrefixSpace(log); log.println("ERROR - The TxDate '" + txDate + "' is less then job current TxDate.");
/*     */ 
/* 292 */       String eventDesc = "[" + job.sysName + "," + job.jobName + "] has received a control file, whose txdate is less then the current txdate";
/* 293 */       ETL.InsertEventLog(con, "RCV", "L", eventDesc);
/* 294 */       ETL.RemoveJobEventRecord(job.sysName + "_" + job.jobName);
/* 295 */       ETL.ShowTime(log); log.println("Generate data file has less than current TxDate message...");
/* 296 */       ETL.WriteMessageNotification(job.sysName, job.jobName, txDate, 
/* 297 */         "Receiving", 
/* 298 */         String.format("Job [%1$s,%2$s] has received a control file, whose txdate is less then the current txdate", new Object[] { 
/* 299 */         job.sysName, job.jobName }), 
/* 300 */         "");
/*     */ 
/* 302 */       return false;
/*     */     }
/*     */ 
/* 305 */     if (!cont.CheckDataFileDuplicate(con, job.sysName, job.jobName, log)) {
/* 306 */       cont.MoveToday(ETL.Auto_home + "/DATA/fail/duplicate/" + job.sysName);
/* 307 */       ETL.RemoveJobEventRecord(job.sysName + "_" + job.jobName);
/*     */ 
/* 309 */       ETL.ShowTime(log); log.println("Generate data file duplicate message...");
/* 310 */       ETL.WriteMessageNotification(job.sysName, job.jobName, txDate, 
/* 311 */         "Receiving", 
/* 312 */         String.format("Job [%1$s,%2$s] has received a control file %3$s with duplicate file", new Object[] { 
/* 313 */         job.sysName, job.jobName, controlFile }), 
/* 314 */         "");
/* 315 */       return false;
/*     */     }
/* 317 */     if (dberr) return true;
/* 318 */     markDataDate(job, txDate);
/* 319 */     ETL.ShowPrefixSpace(log); log.println("All Checks are done");
/* 320 */     ConvertControlFile(job, cont, txDate);
/* 321 */     return dberr;
/*     */   }
/*     */ 
/*     */   private static void markDataDate(JobInfo job, String txDate) {
/* 325 */     if (job.checkCalendar.equalsIgnoreCase("Y")) {
/* 326 */       String sql = String.format("UPDATE DataCalendar SET CheckFlag = 'Y'    WHERE ETL_System = '%1$s' AND ETL_Job = '%2$s'      AND CalendarYear*10000+ CalendarMonth*100 + CalendarDay = %3$s ", new Object[] { 
/* 329 */         job.sysName, job.jobName, txDate });
/*     */       try {
/* 331 */         Statement st = con.createStatement();
/* 332 */         st.execute(sql);
/* 333 */         st.close();
/*     */       } catch (SQLException e) {
/* 335 */         e.printStackTrace();
/*     */       }
/*     */     }
/*     */   }
/*     */ 
/*     */   private static void ConvertControlFile(JobInfo job, ContentControl cont, String txDate) {
/* 341 */     File ctl = new File(cont.nameControl);
/* 342 */     Calendar c = Calendar.getInstance(); c.setTimeInMillis(System.currentTimeMillis());
/* 343 */     String rcvTime = String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", new Object[] { c });
/* 344 */     String baseDir = ctl.getParent();
/* 345 */     String insertCmd = "INSERT INTO ETL_Received_File (ETL_System, ETL_Job, JobSessionID, ReceivedFile, FileSize, ExpectedRecord, ArrivalTime, ReceivedTime, Location, Status) VALUES ('%1$s', '%2$s', %3$s, '%4$s', %5$d, %6$d, '%7$s', '%8$s', '%9$s', '1')";
/*     */ 
/* 349 */     String ctlFileName = String.format(ETL.Auto_home + "/tmp/%1$s_%2$s_%3$s.dir", new Object[] { job.sysName, job.convHead, txDate });
/*     */     try {
/*     */       PrintStream ctl_new;
/*     */       try {
/* 353 */         ctl_new = new PrintStream(new FileOutputStream(ctlFileName));
/*     */       } catch (Exception e) {
/* 355 */         ETL.ShowPrefixSpace(log);
/* 356 */         log.println("Can not create control file:" + ctlFileName);
/* 357 */         return;
/*     */       }
/*     */       PrintStream ctl_new;
/* 359 */       Statement st = con.createStatement();
/* 360 */       for (int i = 0; i < cont.dataCount; ++i) {
/* 361 */         File f = new File(baseDir + "/" + cont.dataFile[i]);
/* 362 */         long fTime = f.lastModified();
/* 363 */         c.setTimeInMillis(fTime);
/* 364 */         String ariTime = String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", new Object[] { c });
/* 365 */         String sql = String.format(insertCmd, new Object[] { job.sysName, job.jobName, job.jobSessionID, cont.dataFile[i], 
/* 366 */           Long.valueOf(cont.fileSize[i]), Long.valueOf(cont.expect[i]), ariTime, rcvTime, ETL.Auto_home + "/DATA/queue" });
/* 367 */         st.execute(sql);
/* 368 */         ctl_new.format("%1$s\t%2$d\n", new Object[] { cont.dataFile[i], Long.valueOf(cont.fileSize[i]) });
/*     */       }
/* 370 */       ctl_new.close();
/* 371 */       st.close();
/*     */     } catch (SQLException e) {
/* 373 */       dberr = true;
/* 374 */       ETL.ShowPrefixSpace(log); log.println("Error - insert source files into ETL_Received_file failed!");
/*     */     }
/*     */ 
/* 377 */     for (int i = 0; i < cont.dataCount; ++i) {
/* 378 */       File dataF = new File(ctl.getParent() + "/" + cont.dataFile[i]);
/* 379 */       File dataQ = new File(ETL.Auto_home + "/DATA/queue/" + cont.dataFile[i]);
/* 380 */       dataF.renameTo(dataQ);
/*     */     }
/* 382 */     File cvtControl = new File(ctlFileName);
/* 383 */     String.format(ETL.Auto_home + "/tmp/%1$s_%2$s_%3$s.dir", new Object[] { job.sysName, job.convHead, txDate });
/* 384 */     cvtControl.renameTo(new File(String.format(ETL.Auto_home + "/DATA/queue/%1$s_%2$s_%3$s.dir", new Object[] { 
/* 385 */       job.sysName, job.convHead, txDate })));
/* 386 */     ETL.ShowPrefixSpace(log);
/* 387 */     File completeDir = new File(ETL.Auto_home + "/DATA/complete/" + job.sysName + "/" + ETL.today);
/* 388 */     if (!completeDir.exists()) completeDir.mkdirs();
/* 389 */     File compF = new File(completeDir.getPath() + "/" + ctl.getName()); compF.delete();
/* 390 */     ctl.renameTo(compF);
/*     */ 
/* 392 */     log.format("Update job status to 'Pending' for %1$s, %2$s, %3$s\n", new Object[] { 
/* 393 */       job.sysName, job.jobName, txDate });
/* 394 */     String sql = String.format("UPDATE ETL_Job SET Last_StartTime = '%1$s', Last_EndTime = null, Last_JobStatus = 'Pending', Last_TXDate = %2$s-19000000, Last_FileCnt= %3$d, Last_CubeStatus = null, ExpectedRecord =%6$d WHERE ETL_System = '%4$s' AND ETL_Job = '%5$s'", new Object[] { 
/* 396 */       rcvTime, txDate, Short.valueOf(cont.dataCount), job.sysName, 
/* 397 */       job.jobName, Long.valueOf(cont.totalExpectedRecord) });
/*     */     try {
/* 399 */       Statement st = con.createStatement();
/* 400 */       st.execute(sql);
/* 401 */       st.close();
/*     */     } catch (SQLException e) {
/* 403 */       dberr = true;
/* 404 */       ETL.ShowPrefixSpace(log); log.println("Error - Update job status failed!");
/*     */     }
/*     */   }
/*     */ 
/*     */   private static boolean CheckDataCalendar(JobInfo job, String txDate) {
/*     */     try {
/* 410 */       Statement st = con.createStatement();
/* 411 */       String sql = "SELECT  CalendarYear*10000 + CalendarMonth*100 + CalendarDay dt, CheckFlag FROM DataCalendar WHERE etl_system='" + 
/* 412 */         job.sysName + "' AND etl_job='" + job.jobName + 
/* 413 */         "'AND dt  <=" + txDate + 
/* 414 */         " ORDER BY dt DESC";
/* 415 */       ResultSet rs = st.executeQuery(sql);
/* 416 */       String cDate = ""; String checkFlag = "X"; String lFlag = " ";
/* 417 */       if (rs.next()) { cDate = rs.getString(1); checkFlag = rs.getString(2); }
/* 418 */       if (rs.next()) lFlag = rs.getString(2);
/* 419 */       rs.close(); st.close();
/*     */ 
/* 421 */       if (cDate.length() == 0) {
/* 422 */         if ((job.jobStatus.equalsIgnoreCase("DONE")) && 
/* 423 */           (job.jobType.equalsIgnoreCase("D"))) {
/* 424 */           Calendar c = ETL.FromTxDate(job.txDate);
/* 425 */           c.add(5, 1);
/* 426 */           String tx1 = String.format("%1$tY%1$tm%1$td", new Object[] { c });
/*     */ 
/* 428 */           return tx1.endsWith(txDate);
/*     */         }
/*     */ 
/* 432 */         return true;
/*     */       }
/* 434 */       if ((!cDate.equals(txDate)) || (!checkFlag.equals("N")))
/* 435 */         return false;
/* 436 */       if (!lFlag.equals("N")) break label267;
/* 437 */       return false; } catch (SQLException ex) {
/* 438 */       dberr = true;
/* 439 */     }label267: return true;
/*     */   }
/*     */ 
/*     */   private static boolean CheckJobFrequency(String frequency, String txDate) {
/* 443 */     Calendar c = ETL.FromTxDate(txDate);
/* 444 */     String[] field = frequency.split("[,]");
/* 445 */     boolean check = false;
/* 446 */     for (int i = 0; (i < field.length) && (!check); ++i) {
/*     */       int t;
/*     */       int t;
/*     */       try { t = Integer.parseInt(field[i]);
/*     */       } catch (NumberFormatException e) {
/* 451 */         t = 0;
/*     */       }
/* 453 */       if (t == 0) {
/* 454 */         check = true;
/* 455 */       } else if (t == -1) {
/* 456 */         c.add(5, 1);
/* 457 */         if (c.get(5) == 1)
/* 458 */           check = true;
/* 459 */       } else if ((t >= 1) && (t <= 31)) {
/* 460 */         if (c.get(5) == t)
/* 461 */           check = true;
/*     */       } else {
/* 463 */         if ((t < 41) || (t > 47) || 
/* 464 */           (c.get(7) != t - 40 + 1)) continue;
/* 465 */         check = true;
/*     */       }
/*     */     }
/* 468 */     return check;
/*     */   }
/*     */ 
/*     */   private static boolean UpdateSourceLastCount(String source) {
/* 472 */     String sqlText = "UPDATE ETL_Job_Source SET LastCount = EXTRACT(DAY FROM DATE) WHERE Source = '" + 
/* 473 */       source + "'";
/*     */     try {
/* 475 */       Statement st = con.createStatement();
/* 476 */       st.execute(sqlText);
/* 477 */       st.close();
/*     */     } catch (SQLException ex) {
/* 479 */       return true;
/*     */     }
/* 481 */     return false;
/*     */   }
/*     */ }

/* Location:           D:\TD_CMP\Automation2.7\DWAuto_server\DWAuto.jar
 * Qualified Name:     etl.RCV
 * JD-Core Version:    0.5.4
 */