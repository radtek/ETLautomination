/*    */ package etl;
/*    */ 
/*    */ import java.io.PrintStream;
/*    */ 
/*    */ public class ShutdownHook
/*    */   implements Runnable
/*    */ {
/*    */   public ShutdownHook()
/*    */   {
/*  8 */     Runtime.getRuntime().addShutdownHook(new Thread(this));
/*    */   }
/*    */ 
/*    */   public void run()
/*    */   {
/* 15 */     ETL.Stop_Flag = true;
/* 16 */     while (!ETL.Finish_Flag)
/* 17 */       Thread.yield();
/*    */   }
/*    */ 
/*    */   public static void main(String[] args)
/*    */   {
/* 32 */     new ShutdownHook();
/* 33 */     System.out.println(">>> Sleeping for 5 seconds, try ctrl-C now if you like.");
/*    */     try {
/* 35 */       Thread.sleep(5000L);
/*    */     }
/*    */     catch (InterruptedException ie) {
/* 38 */       ie.printStackTrace();
/*    */     }
/* 40 */     System.out.println(">>> Slept for 10 seconds and the main thread exited.");
/*    */   }
/*    */ }

/* Location:           D:\TD_CMP\Automation2.7\DWAuto_server\DWAuto.jar
 * Qualified Name:     etl.ShutdownHook
 * JD-Core Version:    0.5.4
 */