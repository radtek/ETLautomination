/*     */ import etl.ETL;
/*     */ import java.io.BufferedReader;
/*     */ import java.io.IOException;
/*     */ import java.io.InputStreamReader;
/*     */ import java.io.PrintStream;
/*     */ import java.sql.Blob;
/*     */ import java.sql.Connection;
/*     */ import java.sql.Date;
/*     */ import java.sql.PreparedStatement;
/*     */ import java.sql.ResultSet;
/*     */ import java.sql.SQLException;
/*     */ import java.sql.Statement;
/*     */ import java.util.Vector;
/*     */ import java.util.zip.DataFormatException;
/*     */ import java.util.zip.Deflater;
/*     */ import java.util.zip.GZIPInputStream;
/*     */ import java.util.zip.Inflater;
/*     */ 
/*     */ public class Upgrade_2_7_1
/*     */ {
/*     */   public static void main(String[] args)
/*     */     throws SQLException, IOException, DataFormatException
/*     */   {
/*  11 */     if (args.length < 1) {
/*  12 */       System.err.println("Please with cfgFile paramenter");
/*  13 */       System.exit(1);
/*     */     }
/*  15 */     ETL.Initialize(args[0]);
/*     */ 
/*  17 */     Connection con = ETL.Connect();
/*  18 */     if (con == null) {
/*  19 */       System.err.println("Can not connect to Automation repository.!!!");
/*  20 */       System.exit(1);
/*     */     }
/*     */ 
/*  23 */     String sqltext = "SELECT TXDate, ETL_System, ETL_Job, JobSessionID, Step_No, LOGContent  FROM ETL_Job_Log ORDER BY 2,3,4 ";
/*     */ 
/*  26 */     Statement s = con.createStatement();
/*     */ 
/*  28 */     ResultSet r = s.executeQuery(sqltext);
/*     */ 
/*  30 */     byte[] Log_Info = new byte[2000];
/*     */ 
/*  32 */     PreparedStatement s1 = con.prepareStatement("INS INTO ETL_Job_LogContent VALUES(?,?,?,?,?,?,?)");
/*     */ 
/*  34 */     while (r.next()) {
/*  35 */       Blob logContent = r.getBlob(6);
/*  36 */       BufferedReader rdr = new BufferedReader(new InputStreamReader(new GZIPInputStream(logContent.getBinaryStream())));
/*  37 */       StringBuffer text = new StringBuffer("");
/*     */       String lineLog;
/*  39 */       while ((lineLog = rdr.readLine()) != null)
/*     */       {
/*     */         String lineLog;
/*  40 */         text.append(lineLog + "\n");
/*     */       }
/*  42 */       Date txDate = r.getDate(1);
/*  43 */       String sys = r.getString(2);
/*  44 */       String job = r.getString(3);
/*  45 */       int sess = r.getInt(4);
/*  46 */       short stepN = r.getShort(5);
/*  47 */       short seq_no = 0;
/*     */ 
/*  49 */       Deflater compresser = new Deflater();
/*  50 */       compresser.setInput(text.toString().getBytes("UTF-8"));
/*  51 */       compresser.finish();
/*     */       while (true) {
/*  53 */         int cLen = compresser.deflate(Log_Info);
/*  54 */         if (cLen == 0) break;
/*  55 */         byte[] tLog = new byte[cLen];
/*  56 */         System.arraycopy(Log_Info, 0, tLog, 0, cLen);
/*  57 */         s1.setDate(1, txDate);
/*  58 */         s1.setString(2, sys);
/*  59 */         s1.setString(3, job);
/*  60 */         s1.setInt(4, sess);
/*  61 */         s1.setShort(5, stepN);
/*  62 */         s1.setShort(6, seq_no);
/*  63 */         s1.setBytes(7, tLog);
/*  64 */         s1.execute();
/*  65 */         seq_no = (short)(seq_no + 1);
/*     */       }
/*     */     }
/*     */ 
/*  69 */     r.close();
/*  70 */     r = s.executeQuery("SELECT LOG_Info FROM ETL_Job_LogContent WHERE ETL_System='TST' AND ETL_JOB='ENV_DSPL' AND JobSessionID=220 AND Step_No=90 ORDER BY Seq_No");
/*     */ 
/*  73 */     Inflater decompresser = new Inflater();
/*  74 */     int Log_len = 0;
/*  75 */     Vector v1 = new Vector();
/*     */ 
/*  77 */     while (r.next()) {
/*  78 */       Log_Info = r.getBytes(1);
/*  79 */       decompresser.setInput(Log_Info);
/*     */       while (true) {
/*  81 */         int resultLength = decompresser.inflate(Log_Info);
/*  82 */         if (resultLength == 0) break;
/*  83 */         byte[] tLog = new byte[resultLength];
/*  84 */         System.arraycopy(Log_Info, 0, tLog, 0, resultLength);
/*  85 */         v1.add(tLog);
/*     */       }
/*     */     }
/*  88 */     s.close();
/*  89 */     Log_Info = new byte[decompresser.getTotalOut()];
/*  90 */     Log_len = 0;
/*  91 */     decompresser.end();
/*  92 */     for (int i = 0; i < v1.size(); ++i) {
/*  93 */       byte[] abLog = (byte[])v1.elementAt(i);
/*  94 */       System.arraycopy(abLog, 0, Log_Info, Log_len, abLog.length);
/*  95 */       Log_len += abLog.length;
/*     */     }
/*     */ 
/*  98 */     String logText = new String(Log_Info, "UTF-8");
/*  99 */     System.out.print(logText);
/* 100 */     con.close();
/*     */   }
/*     */ }

/* Location:           D:\TD_CMP\Automation2.7\DWAuto_server\DWAuto.jar
 * Qualified Name:     Upgrade_2_7_1
 * JD-Core Version:    0.5.4
 */