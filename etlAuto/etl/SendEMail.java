/*    */ package etl;
/*    */ 
/*    */ import java.util.Properties;
/*    */ import javax.mail.Message.RecipientType;
/*    */ import javax.mail.MessagingException;
/*    */ import javax.mail.Multipart;
/*    */ import javax.mail.Session;
/*    */ import javax.mail.Transport;
/*    */ import javax.mail.internet.AddressException;
/*    */ import javax.mail.internet.InternetAddress;
/*    */ import javax.mail.internet.MimeBodyPart;
/*    */ import javax.mail.internet.MimeMessage;
/*    */ import javax.mail.internet.MimeMultipart;
/*    */ 
/*    */ public class SendEMail
/*    */ {
/*    */   public static boolean sendMail(String stmpServer, String fromEmail, String toEmail, String fromEmailPassword, String title, String content)
/*    */   {
/* 21 */     boolean check = false;
/*    */ 
/* 23 */     System.setProperty("mail.smtp.auth", "true");
/*    */ 
/* 25 */     Properties props = System.getProperties();
/* 26 */     props.put("mail.smtp.host", stmpServer);
/*    */ 
/* 28 */     Session session = Session.getInstance(props, null);
/* 29 */     MimeMessage message = new MimeMessage(session);
/* 30 */     MimeBodyPart messageBodyPart = new MimeBodyPart();
/* 31 */     Multipart multipart = new MimeMultipart();
/*    */     try
/*    */     {
/* 34 */       message.setFrom(new InternetAddress(fromEmail));
/* 35 */       message.addRecipient(Message.RecipientType.TO, 
/* 36 */         new InternetAddress(toEmail));
/* 37 */       message.setSubject(title);
/*    */ 
/* 39 */       messageBodyPart.setText(content);
/* 40 */       multipart.addBodyPart(messageBodyPart);
/*    */ 
/* 42 */       message.setContent(multipart);
/* 43 */       message.saveChanges();
/*    */ 
/* 45 */       Transport transport = session.getTransport("smtp");
/*    */ 
/* 47 */       transport.connect(stmpServer, 25, fromEmail, fromEmailPassword);
/* 48 */       transport.sendMessage(message, message.getAllRecipients());
/* 49 */       transport.close();
/*    */ 
/* 51 */       check = true;
/*    */     }
/*    */     catch (AddressException e) {
/* 54 */       e.printStackTrace();
/* 55 */       check = false;
/*    */     } catch (MessagingException e) {
/* 57 */       e.printStackTrace();
/* 58 */       check = false;
/*    */     }
/* 60 */     return check;
/*    */   }
/*    */ }

/* Location:           D:\TD_CMP\Automation2.7\DWAuto_server\DWAuto.jar
 * Qualified Name:     etl.SendEMail
 * JD-Core Version:    0.5.4
 */