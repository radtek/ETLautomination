# -*- coding: utf-8 -*-
import os
import sys
import re
import datetime
import time
import logging
from glob import glob
import cx_Oracle as oracle
from ETL import ETL
from ControlFilter import ControlFilter
from JobInfo import JobInfo


logging.basicConfig(level=logging.INFO)

class RCV(object):
	conn = None
	rcvDir = ''
	dberr = False
	StableCheckSize = None

	def RefreshSizeStable(self):
		for file in self.StableCheckSize:
			if os.path.exists(file):
				continue
			self.StableCheckSize.pop(file,'')
	
	def IsSizeStable(self,fileName):
		fileLen = os.path.getsize(fileName)
		oldLen = self.StableCheckSize.get(fileName)
		if oldLen is None :
			self.StableCheckSize[fileName] = fileLen
			return False
		if oldLen < fileLen:
			self.StableCheckSize[fileName] = fileLen
			return False
		return True
	
	def GetSrc(self,controlFile):
		return controlFile[4:len(controlFile)-8]

	def GetTXDate(self,controlFile):
		return controlFile[len(controlFile)-8:]

	def CheckReceiveDir(self,conn):
		rcvDir = ETL.Auto_home + "/DATA/receive"
		dberr = False

		logging.info(ETL.ShowTime())
		logging.info("Checking receiving directory '" + rcvDir + "...")

		fns = glob(rcvDir+ControlFilter(1).getPattern())
		if fns is None:
			logging.info("The Path is not exist.")
			return
		RefreshSizeStable()
		controlList = [None] * len(fns)
		lstLen = 0
		j = 0
		for i in range(0,len(lstLen)):
			if not IsSizeStable(fns[i]):
				continue
			source = GetSrc(os.path.basename(fns[i]))
			for j in range(j,lstLen):
				if self.source == GetSrc(controlList[j]):
					break
			if j == lstLen:				
				controlList[lstLen] = os.path.basename(fns[i])
				lstLen = lstLen + 1
			else:
				tx1 = GetSrc(controlList[j])
				if cmp(GetTXDate(os.path.basename(fns[i])),tx1) < 0:
					controlList[j] = os.path.basename(fns[i])

		if lstLen == 0:
			return

		## 这里需要实现对 fns 列表的排序操作，暂时先不实现
		for i in range(0,lstLen):
			if ProcessControlFile(controlList[i]):
				return

	def CheckRelatedJobStatus(self,job,txDate):
		ret = True
		logging.info(ETL.ShowPrefixSpace())
		logging.info("Check related job status...")
		sqlText = """
		select  B.ETL_System, B.ETL_Job, A.CheckMode, Last_JobStatus, Last_TXDate, JobType, CheckCalendar
		,to_date('%s','YYYYMMDD')-Last_TXDate as dayDiff
		from ETL_RelatedJob A, ETL_JOB B
		WHERE A.etl_system = '%s' AND A.etl_job='%s'
		AND A.RelatedSystem = B.ETL_System  AND A.RelatedJob = B.ETL_Job
		""" %(txDate,job.sysName,job.jobName)
		
		pass					
						







if __name__ == '__main__':
	pass	

