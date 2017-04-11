import os
import sys
import re
import time
import datetime
import cx_Oracle as oracle
import logging
import glob
from ETL import ETL
from ControlFilter import ControlFilter

logging.basicConfig(level = logging.INFO)

class MSG:
	dberr = None
	sys = None
	job = None
	txdate = None
	msgType = None
	title = None
	content = None
	logName = None
	msgTypeChar = None

	def __init__(self,msgF):
		with open(msgF,'r') as file:
			line = file.readline()
			if line is None or line != 'Automation Message Notification':
				return

			line = file.readline()
			if line is None:
				return
			words = line.split(" ")
			if len(words) > 1:
				self.sys = words[0]

			line = file.readline()
			if line is None:
				return
			words = line.split(" ")
			if len(words) > 1:
				self.job = words[0]

			line = file.readline()
			if line is None:
				return	
			words = line.split(" ")
			if len(words) > 1:
				self.txdate = words[0]

			line = file.readline()
			if line is None:
				return	
			words = line.split(" ")
			if len(words) > 1:
				self.msgType = words[0]

			if self.msgType == 'Done':
				self.msgTypeChar = 'D'
			if self.msgType == 'Failed':
				self.msgTypeChar = 'F'
			if self.msgType == 'Missing':
				self.msgTypeChar = 'M'
			if self.msgType == 'Receiving':
				self.msgTypeChar = 'R'
			if self.msgType == 'RecordError':
				self.msgTypeChar = 'E'

			line = file.readline()
			if line is None:
				return	
			words = line.split(" ")
			if len(words) > 1:
				self.logName = words[0]	

			line = file.readline()
			if line is None:
				return	
			posSub = line.find(' ')
			if posSub > 1:
				self.title = line[posSub+1:]

			line = file.readline()
			if line is None:
				return	
			posSub = line.find(' ')
			if posSub > 1:
				self.content = line[posSub+1:]
			while(True):
				line = file.readline()
				if line is None:
					break
				if len(line) != 0:
					self.content = self.content + '\n' + line

	def CheckMessageDir(self,conn):
		msgDir = ETL.Auto_home + "DATA/message"
		dberr = False
		logging.info(ETL.ShowTime())
		logging.info("Checking Message directory '" + msgDir + "'...")
		if not os.path.exists(msgDir):
			logging.info(ETL.ShowTime())
			logging.info("The Message directory is not exist!!!!")
			return dberr
		pattern = ControlFilter(3).getPattern()

		fns = glob.glob(msgDir+'/'+pattern)
		for i in range(0,len(fns)):
			logging.info(ETL.ShowTime())
			logging.info("Processing message notification file '" + os.path.basename(fns[i]) + "'")
			msgCtl = MSG(fns[i])
			if msgCtl.msgType == ' ':
				logging.info(ETL.ShowPrefixSpace())
				logging.info("Unknow message type '" + msgCtl.msgtype + "'")
				os.unlink(fns[i])
			elif msgCtl.ProcessControlFile(conn):
				os.unlink(fns[i])

		return dberr

	def ProcessControlFile(self,conn):
		ret = True
		logging.info(ETL.ShowTime())
		logging.info("Message notification for [" + self.sys + "," + self.job + "] msgType=" + self.msgtype)
		sqlText = """
		select B.UserName, B.Email MailAddr, B.Mobile, AttachLog, A.Email,ShortMessage, MessageSubject, MessageContent
		from ETL_Notification A, ETL_User B
		WHERE ETL_System='%s' AND ETL_Job = '%s' AND timing = '%s' and DestType='U' and A.UserName = B.UserName AND B.Status = '1'
		union
		SELECT C.UserName, C.Email MailAddr, C.Mobile, AttachLog, A.Email,ShortMessage, MessageSubject, MessageContent 
		from ETL_Notification A, ETL_GroupMember B, ETL_User C 
		WHERE ETL_System='%s' AND ETL_Job = '%s' AND timing = '%s' and DestType='G' AND A.GroupName = B.GroupName  and B.UserName = C.UserName AND C.Status = '1' 
		""" %(self.sys,self,job,self.msgTypeChar,self.sys,self,job,self.msgTypeChar)
		try:
            cursor = con.cursor()
            cursor.execute(sqlText)
            result = cursor.fetchall()
            for resTuple in result:
                emailAddr = resTuple[1] if resTuple is not None else ''
                mobile = resTuple[2] if resTuple is not None else ''
                txt = resTuple[4] if resTuple is not None else 'N'
                ynEmail = (txt == 'Y')
                txt = resTuple[5] if resTuple is not None else 'N'
                ynSMS = (txt == 'Y')
                txt = resTuple[3] if resTuple is not None else 'N'
                ynAttachLog = (txt == 'Y')
                msubject = resTuple[6] if resTuple is not None else ''
                msubject = "Automation Message Notification" if len(msubject) == 0
                mcontent = resTuple[7] if resTuple is not None else ''
                mcontent = self.content if len(mcontent) == 0
                userName = resTuple[0]
                logging.info(ETL.ShowPrefixSpace())
                logging.info("Send notification to user [" + userName + "]")
                logging.info(ETL.ShowPrefixSpace())
                logging.info("Attached:" + ynAttachLog + " Email:" + ynEmail + " SMS:" + ynSMS)
                if ynSMS:
                	ret = sendSMS(mobile, msubject)
                if ynEmail:
                	if len(self.logName)>0 and ynAttachLog:
                		mcontent = mcontent + "\n==================== Job Log ===================\n"
                		mcontent = mcontent + ETL.getLogContentText(con, self.logName)
                	ret = sendMail(emailAddr, msubject, mcontent)
                	logging.info(ETL.ShowPrefixSpace())
                	logging.info("Send Email to " + userName + " Email:" + emailAddr + ", retCode=" + ret)
        except oracle.DatabaseError as e:
            logging.error("Database Error Message: "+str(e.message))
            self.dberr = True
            ret = False

        return ret    

	def sendMail(self,emailAddr,msubject,mcontent):
		# 暂时先不实现发邮件
		return True

	def sendSMS(self,mobile,mcontent):
		# 暂时先不实现发短信
		return True			 
			



if __name__ == '__main__':
	print ETL.Auto_url