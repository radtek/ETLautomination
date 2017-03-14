/*     */ import etl.ETL;
/*     */ import etl.HouseKeeping;
/*     */ import etl.JobRunner;
/*     */ import etl.MSG;
/*     */ import etl.Master;
/*     */ import etl.RCV;
/*     */ import etl.ShutdownHook;
/*     */ import java.io.FileNotFoundException;
/*     */ import java.io.FileOutputStream;
/*     */ import java.io.PrintStream;
/*     */ import java.sql.Connection;
/*     */ import java.sql.SQLException;
/*     */ import java.util.Calendar;
/*     */ import java.util.concurrent.atomic.AtomicInteger;
/*     */ 
/*     */ public class DWAuto
/*     */ {
/*   9 */   static int lastDay = -1;
/*  10 */   static PrintStream rcvLog = System.out; static PrintStream masLog = System.out; static PrintStream agentLog = System.out;
/*  11 */   static PrintStream slaveLog = System.out; static PrintStream msgLog = System.out;
/*     */ 
/*     */   static void createLog() {
/*  14 */     Calendar c = Calendar.getInstance();
/*  15 */     int today = c.get(5);
/*  16 */     String dateStr = String.format("%1$tY%1$tm%1$td", new Object[] { c });
/*     */ 
/*  18 */     if (today != lastDay) {
/*  19 */       if (rcvLog != System.out) rcvLog.close();
/*  20 */       String fileName = ETL.Auto_home + "/LOG/etlrcv_" + dateStr + ".log";
/*     */       try {
/*  22 */         rcvLog = new PrintStream(new FileOutputStream(fileName, true));
/*     */       } catch (FileNotFoundException e) {
/*  24 */         e.printStackTrace(); rcvLog = System.out;
/*     */       }
/*  26 */       ETL.PrintVersionInfo(rcvLog, "DWAuto-rcv");
/*     */ 
/*  28 */       if (masLog != System.out) masLog.close();
/*  29 */       fileName = ETL.Auto_home + "/LOG/etlmas_" + dateStr + ".log";
/*     */       try {
/*  31 */         masLog = new PrintStream(new FileOutputStream(fileName, true));
/*     */       } catch (FileNotFoundException e) {
/*  33 */         e.printStackTrace(); masLog = System.out;
/*     */       }
/*  35 */       ETL.PrintVersionInfo(masLog, "DWAuto-Master");
/*     */ 
/*  37 */       if (msgLog != System.out) msgLog.close();
/*  38 */       fileName = ETL.Auto_home + "/LOG/etlmsg_" + dateStr + ".log";
/*     */       try {
/*  40 */         msgLog = new PrintStream(new FileOutputStream(fileName, true));
/*     */       } catch (FileNotFoundException e) {
/*  42 */         e.printStackTrace(); msgLog = System.out;
/*     */       }
/*  44 */       ETL.PrintVersionInfo(masLog, "DWAuto-msg");
/*     */     }
/*  46 */     lastDay = today;
/*     */   }
/*     */ 
/*     */   public static void main(String[] args)
/*     */   {
/*  51 */     if (args.length < 1) {
/*  52 */       System.err.println("Please with cfgFile paramenter");
/*  53 */       System.exit(1);
/*     */     }
/*  55 */     ETL.Initialize(args[0]);
/*     */ 
/*  57 */     Connection con = ETL.Connect();
/*  58 */     if (con == null) {
/*  59 */       System.err.println("Can not connect to Automation repository.!!!");
/*  60 */       System.exit(1);
/*     */     }
/*  62 */     createLog();
/*  63 */     int agentPort = ETL.getAgentPort(con, ETL.Auto_server);
/*  64 */     if (agentPort == 0) {
/*  65 */       System.err.println("This server is not configured in Automation repository.");
/*  66 */       System.exit(1);
/*     */     }
/*  68 */     System.out.println("AGENTPORT:" + agentPort); System.out.flush();
/*  69 */     boolean heartOK = ETL.HearBeat();
/*  70 */     if (!heartOK) {
/*  71 */       System.err.println("This is not first instance of DW Automation for the repository.!!!");
/*  72 */       System.exit(1);
/*     */     }
/*     */ 
/*  75 */     new ShutdownHook();
/*  76 */     new JobRunner(1);
/*  77 */     new JobRunner(agentPort);
/*     */ 
/*  79 */     RCV.CheckReceiveDir(con, rcvLog);
/*  80 */     Master.ResetRunningJob(con, masLog);
/*     */     while (true)
/*     */     {
/*  83 */       ETL.Sleep(ETL.AutoSleep);
/*  84 */       if (ETL.Stop_Flag) {
/*  85 */         System.out.println("DW Automation is stopped.");
/*  86 */         break;
/*     */       }
/*  88 */       createLog();
/*  89 */       if (!ETL.ping(con))
/*     */         try {
/*  91 */           if (con != null) con.close(); con = null;
/*  92 */           con = ETL.Connect(); } catch (SQLException e) {
/*  93 */           con = null;
/*     */         }
/*  94 */       if (con == null) {
/*  95 */         ETL.Sleep(30);
/*     */       }
/*     */ 
/*  99 */       Master.doSchedule(con, masLog);
/* 100 */       if (ETL.serviceHold != 1) {
/* 101 */         Master.CheckJobQueue(con, masLog);
/* 102 */         RCV.CheckReceiveDir(con, rcvLog);
/* 103 */         Master.CheckPendingJob(con, masLog);
/* 104 */         MSG.CheckMessageDir(con, msgLog);
/* 105 */         HouseKeeping.CleanupAll(con);
/*     */       }
/* 107 */       ETL.HearBeat();
/* 108 */       System.gc();
/*     */     }
/* 110 */     if (JobRunner.RunningCount.get() > 0) {
/* 111 */       System.err.print("There are runnning jobs in Automation system!!!!\n");
/* 112 */       int w = 60;
/* 113 */       while (JobRunner.RunningCount.get() > 0) {
/* 114 */         ETL.Sleep(1); --w;
/* 115 */         if (w == 0) break;
/*     */       }
/*     */     }
/* 118 */     ETL.Finish_Flag = true;
/* 119 */     if (JobRunner.RunningCount.get() == 0) {
/* 120 */       System.out.println("All works has been done.");
/* 121 */       System.exit(0);
/*     */     } else {
/* 123 */       System.out.println("Maybe some job canceled!!!.");
/* 124 */       System.exit(0);
/*     */     }
/*     */   }
/*     */ }

/* Location:           D:\TD_CMP\Automation2.7\DWAuto_server\DWAuto.jar
 * Qualified Name:     DWAuto
 * JD-Core Version:    0.5.4
 */