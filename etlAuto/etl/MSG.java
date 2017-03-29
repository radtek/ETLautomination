/*     */ package etl;
/*     */ 
/*     */ import java.io.BufferedReader;
/*     */ import java.io.File;
/*     */ import java.io.FileInputStream;
/*     */ import java.io.FileNotFoundException;
/*     */ import java.io.FileReader;
/*     */ import java.io.IOException;
/*     */ import java.io.PrintStream;
/*     */ import java.sql.Connection;
/*     */ import java.sql.ResultSet;
/*     */ import java.sql.SQLException;
/*     */ import java.sql.Statement;
/*     */ import java.util.Arrays;
/*     */ import java.util.Properties;
/*     */ 
/*     */ public class MSG
/*     */ {
/*     */   private static boolean dberr;
/*     */   String sys;
/*     */   String job;
/*     */   String txdate;
/*     */   String msgtype;
/*     */   String title;
/*     */   String content;
/*     */   String logName;
/*     */   char msgTypeChar;
/*     */ 
/*     */   MSG(File msgF)
/*     */   {
/*  14 */     this.sys = ""; this.job = ""; this.txdate = ""; this.msgtype = ""; this.title = ""; this.content = ""; this.logName = "";
/*  15 */     this.msgTypeChar = ' ';
/*     */     try
/*     */     {
/*  18 */       BufferedReader in = new BufferedReader(new FileReader(msgF));
/*  19 */       String line = in.readLine();
/*  20 */       if ((line == null) || (!line.equals("Automation Message Notification"))) {
/*  21 */         return;
/*     */       }
/*  23 */       line = in.readLine(); if (line == null) return;
/*  24 */       String[] words = line.split(" "); if (words.length > 1) this.sys = words[1];
/*     */ 
/*  26 */       line = in.readLine(); if (line == null) return;
/*  27 */       words = line.split(" "); if (words.length > 1) this.job = words[1];
/*     */ 
/*  29 */       line = in.readLine(); if (line == null) return;
/*  30 */       words = line.split(" "); if (words.length > 1) this.txdate = words[1];
/*     */ 
/*  32 */       line = in.readLine(); if (line == null) return;
/*  33 */       words = line.split(" "); if (words.length > 1) this.msgtype = words[1];
/*     */ 
/*  35 */       if (this.msgtype.equals("Done")) this.msgTypeChar = 'D';
/*  36 */       if (this.msgtype.equals("Failed")) this.msgTypeChar = 'F';
/*  37 */       if (this.msgtype.equals("Missing")) this.msgTypeChar = 'M';
/*  38 */       if (this.msgtype.equals("Receiving")) this.msgTypeChar = 'R';
/*  39 */       if (this.msgtype.equals("RecordError")) this.msgTypeChar = 'E';
/*     */ 
/*  41 */       line = in.readLine(); if (line == null) return;
/*  42 */       words = line.split(" ");
/*  43 */       if (words.length > 1) this.logName = words[1];
/*     */ 
/*  45 */       line = in.readLine(); if (line == null) return;
/*  46 */       int posSub = line.indexOf(' ');
/*  47 */       if (posSub > 1) this.title = line.substring(posSub + 1);
/*     */ 
/*  49 */       line = in.readLine(); if (line == null) return;
/*  50 */       posSub = line.indexOf(' ');
/*  51 */       if (posSub > 1) this.content = line.substring(posSub + 1); while (true)
/*     */       {
/*  53 */         line = in.readLine();
/*  54 */         if (line == null) break;
/*  55 */         if (line.length() != 0);
/*  56 */         this.content = (this.content + "\n" + line);
/*     */       }
/*  58 */       in.close();
/*     */     } catch (FileNotFoundException e) {
/*  60 */       e.printStackTrace();
/*     */     } catch (IOException e) {
/*  62 */       e.printStackTrace();
/*     */     }
/*     */   }
/*     */ 
/*     */   public static boolean CheckMessageDir(Connection con, PrintStream log)
/*     */   {
/*  69 */     String msgDir = ETL.Auto_home + "/DATA/message";
/*  70 */     dberr = false;
/*     */ 
/*  72 */     ETL.ShowTime(log); log.println("Checking Message directory '" + msgDir + "'...");
/*  73 */     File f = new File(msgDir);
/*  74 */     if (!f.exists()) {
/*  75 */       ETL.ShowTime(log); log.println("The Message directory is not exist!!!!");
/*  76 */       return dberr;
/*     */     }
/*  78 */     File[] fns = f.listFiles(new ControlFilter(3));
/*  79 */     Arrays.sort(fns);
/*     */ 
/*  81 */     for (int i = 0; i < fns.length; ++i) {
/*  82 */       if (ETL.Stop_Flag) break;
/*  83 */       ETL.ShowTime(log); log.println("Processing message notification file '" + fns[i].getName() + "'");
/*  84 */       MSG msgCtl = new MSG(fns[i]);
/*  85 */       if (msgCtl.msgTypeChar == ' ') {
/*  86 */         ETL.ShowPrefixSpace(log); log.println("Unknow message type '" + msgCtl.msgtype + "'");
/*  87 */         fns[i].delete();
/*     */       }
/*  90 */       else if (msgCtl.ProcessControlFile(con, log)) {
/*  91 */         fns[i].delete();
/*     */       }
/*     */     }
/*  94 */     return dberr;
/*     */   }
/*     */ 
/*     */   private boolean ProcessControlFile(Connection con, PrintStream log) {
/*  98 */     boolean ret = true;
/*  99 */     ETL.ShowPrefixSpace(log);
/* 100 */     log.println("Message notification for [" + this.sys + "," + this.job + "] msgType=" + this.msgtype);
/* 101 */     String sql = String.format(
/* 102 */       "SELECT B.UserName, B.Email MailAddr, B.Mobile, AttachLog, A.Email,             \n  ShortMessage, MessageSubject, MessageContent                     \nFROM ETL_Notification A, ETL_User B                                \nWHERE ETL_System='%1$s' AND ETL_Job = '%2$s' AND timing = '%3$c'   \n  AND DestType='U'                                                 \n  AND A.UserName = B.UserName AND B.Status = '1'                   \nUNION                                                              \nSELECT C.UserName, C.Email MailAddr, C.Mobile, AttachLog, A.Email,             \n  ShortMessage, MessageSubject, MessageContent                     \nFROM ETL_Notification A, ETL_GroupMember B, ETL_User C             \nWHERE ETL_System='%1$s' AND ETL_Job = '%2$s' AND timing = '%3$c'   \n  AND DestType='G' AND A.GroupName = B.GroupName                   \n  AND B.UserName = C.UserName AND C.Status = '1'                   \n", new Object[] { 
/* 115 */       this.sys, this.job, Character.valueOf(this.msgTypeChar) });
/*     */     try {
/* 117 */       Statement s = con.createStatement();
/* 118 */       ResultSet r = s.executeQuery(sql);
/* 119 */       while (r.next()) {
/* 120 */         String emailAddr = r.getString("MailAddr"); if (r.wasNull()) emailAddr = "";
/* 121 */         String mobile = r.getString("Mobile"); if (r.wasNull()) mobile = "";
/* 122 */         String txt = r.getString("Email"); if (r.wasNull()) txt = "N";
/* 123 */         boolean ynEmail = txt.equals("Y");
/* 124 */         txt = r.getString("ShortMessage"); if (r.wasNull()) txt = "N";
/* 125 */         boolean ynSMS = txt.equals("Y");
/* 126 */         txt = r.getString("AttachLog"); if (r.wasNull()) txt = "N";
/* 127 */         boolean ynAttachLog = txt.equals("Y");
/* 128 */         String msubject = r.getString("MessageSubject"); if (r.wasNull()) msubject = "";
/* 129 */         if (msubject.length() == 0) msubject = new String(this.title);
/* 130 */         if (msubject.length() == 0) msubject = "Automation Message Notification";
/* 131 */         String mcontent = r.getString("MessageContent"); if (r.wasNull()) mcontent = "";
/* 132 */         if (mcontent.length() == 0) mcontent = new String(this.content);
/* 133 */         String userName = r.getString("UserName");
/* 134 */         ETL.ShowPrefixSpace(log);
/* 135 */         log.println("Send notification to user [" + userName + "]");
/* 136 */         ETL.ShowPrefixSpace(log);
/* 137 */         log.println("Attached:" + ynAttachLog + " Email:" + ynEmail + " SMS:" + ynSMS);
/* 138 */         if (ynSMS) {
/* 139 */           ret = sendSMS(mobile, msubject);
/*     */         }
/* 141 */         if (ynEmail) {
/* 142 */           if ((this.logName.length() > 0) && (ynAttachLog)) {
/* 143 */             mcontent = mcontent + "\n==================== Job Log ===================\n";
/* 144 */             mcontent = mcontent + ETL.getLogContentText(con, this.logName);
/*     */           }
/* 146 */           ret = sendMail(emailAddr, msubject, mcontent);
/* 147 */           ETL.ShowPrefixSpace(log);
/* 148 */           log.println("Send Email to " + userName + " Email:" + emailAddr + ", retCode=" + ret);
/*     */         }
/*     */       }
/* 151 */       r.close(); s.close(); } catch (SQLException e) {
/* 152 */       dberr = true; ret = false;
/* 153 */     }return ret;
/*     */   }
/*     */ 
/*     */   private boolean sendMail(String emailAddr, String msubject, String mcontent) {
/* 157 */     String pass = "nopass";
/* 158 */     Properties cfgVar = new Properties();
/*     */     try {
/* 160 */       cfgVar.load(new FileInputStream(ETL.cfgInit)); } catch (Exception e) {
/* 161 */       return false;
/* 162 */     }String smtpServer = cfgVar.getProperty("AUTO_SMTP_SERVER", "127.0.0.1");
/* 163 */     String sender = cfgVar.getProperty("AUTO_SENDER", "yongfu_wang@sina.com/qqqq");
/* 164 */     String[] w = sender.split("/");
/* 165 */     if (w.length > 1) {
/* 166 */       pass = w[1]; sender = w[0];
/*     */     }
/* 168 */     return SendEMail.sendMail(smtpServer, sender, emailAddr, pass, msubject, mcontent);
/*     */   }
/*     */ 
/*     */   private boolean sendSMS(String mobile, String mcontent) {
/* 172 */     return true;
/*     */   }
/*     */ }

/* Location:           D:\TD_CMP\Automation2.7\DWAuto_server\DWAuto.jar
 * Qualified Name:     etl.MSG
 * JD-Core Version:    0.5.4
 */
