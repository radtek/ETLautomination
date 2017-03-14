import os
import sys
import time
import datetime
import logging
import cx_Oracle as oracle

logging.basicConfig(level=logging.INFO)

class JobInfo(object):
    """docstring for ClassName"""
    sysName = None
    jobName = None
    convHead = None
    checkLastStatus = None
    enable = None
    jobSessionID = None
    jobStatus = None
    autoFilter = None
    txDate = None
    frequency = None
    checkCalendar = None
    jobType = None

    def __init__(self,con,jobSource):
        sqlText = "SELECT A.etl_system, A.etl_job, B.Conv_File_Head, A.CheckLastStatus, A.Enable, A.JobSessionID, "+ \
                  "A.Last_JobStatus, B.AutoFilter,A.Last_TXDate, A.Frequency, A.CheckCalendar, A.JobType " + \
                  " FROM etl_job A, etl_job_source B WHERE A.etl_system=B.etl_system AND A.etl_job=B.etl_job AND B.source ='" + \
                  jobSource + "'"
        try:
            cursor.execute(sqlText)
            result = cursor.fetchall()
            for listInfo in result:
                self.sysName = listInfo[0]
                self.jobName = listInfo[1]
                self.convHead = listInfo[2]
                self.checkLastStatus = listInfo[3] if listInfo[3] else ''
                self.enable = listInfo[4] if listInfo[4] else ''
                self.jobSessionID = listInfo[5] if listInfo[5] else '0'
                self.jobStatus = listInfo[6] if listInfo[6] else ''
                self.autoFilter = listInfo[7] if listInfo[7] else ''
                self.txDate = listInfo[8] if listInfo[8] else ''
                self.txDate = self.txDate.replace("-","")
                self.frequency = listInfo[9] if listInfo[9] else ''
                self.checkCalendar = listInfo[10] if listInfo[10] else ''
                self.jobType = listInfo[11] if listInfo[11] else ''

        except oracle.DatabaseError as e:
            logging.error("Oracle-Error-Message: "+str(e.message))
            cursor.close()
            con.close()
        finally:
            cursor.close()
            con.close()         

if __name__ == '__main__':
    pass        
