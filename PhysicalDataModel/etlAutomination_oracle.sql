/*==============================================================*/
/* DBMS name:      ORACLE Version 11g                           */
/* Created on:     2017-2-22 14:58:53                           */
/*==============================================================*/


drop table DataCalendar cascade constraints;

drop table DataCalendarYear cascade constraints;

drop table ETL_Event cascade constraints;

drop table ETL_GroupMember cascade constraints;

drop table ETL_JOB_STEP cascade constraints;

drop table ETL_Job cascade constraints;

drop table ETL_Job_Dependency cascade constraints;

drop table ETL_Job_Group cascade constraints;

drop table ETL_Job_GroupChild cascade constraints;

drop table ETL_Job_Log cascade constraints;

drop table ETL_Job_Queue cascade constraints;

drop table ETL_Job_Source cascade constraints;

drop table ETL_Job_Status cascade constraints;

drop table ETL_Job_Stream cascade constraints;

drop table ETL_Job_TimeWindow cascade constraints;

drop table ETL_Job_Trace cascade constraints;

drop table ETL_Locks cascade constraints;

drop table ETL_Notification cascade constraints;

drop table ETL_Received_File cascade constraints;

drop table ETL_Record_Log cascade constraints;

drop table ETL_RelatedJob cascade constraints;

drop table ETL_Server cascade constraints;

drop table ETL_Sys cascade constraints;

drop table ETL_TimeTrigger cascade constraints;

drop table ETL_TimeTrigger_Calendar cascade constraints;

drop table ETL_TimeTrigger_Monthly cascade constraints;

drop table ETL_TimeTrigger_Weekly cascade constraints;

drop table ETL_User cascade constraints;

drop table ETL_UserGroup cascade constraints;

/*==============================================================*/
/* Table: DataCalendar                                          */
/*==============================================================*/
create table DataCalendar 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   CalendarYear         NUMBER               not null,
   SeqNum               number               not null,
   CalendarMonth        NUMBER,
   CalendarDay          NUMBER,
   CheckFlag            CHAR(1),
   constraint PK_DATACALENDAR primary key (ETL_System, ETL_Job, CalendarYear, SeqNum)
);

comment on table DataCalendar is
'数据日历表';

/*==============================================================*/
/* Table: DataCalendarYear                                      */
/*==============================================================*/
create table DataCalendarYear 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   CalendarYear         NUMBER               not null,
   constraint PK_DATACALENDARYEAR primary key (ETL_System, ETL_Job, CalendarYear)
);

comment on table DataCalendarYear is
'数据年历表';

/*==============================================================*/
/* Table: ETL_Event                                             */
/*==============================================================*/
create table ETL_Event 
(
   EventID              VARCHAR2(20),
   EventStatus          CHAR(1),
   Severity             CHAR(1),
   Description          varchar2(200),
   LogTime              varchar2(19),
   CloseTime            varchar2(19),
   constraint PK_ETL_EVENT primary key ()
);

comment on table ETL_Event is
'ETL事件表';

/*==============================================================*/
/* Table: ETL_GroupMember                                       */
/*==============================================================*/
create table ETL_GroupMember 
(
   UserName             VARCHAR2(15)         not null,
   GroupName            VARCHAR2(15)         not null,
   constraint PK_ETL_GROUPMEMBER primary key (UserName, GroupName)
);

comment on table ETL_GroupMember is
'ETL组表';

/*==============================================================*/
/* Table: ETL_JOB_STEP                                          */
/*==============================================================*/
create table ETL_JOB_STEP 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   Step_No              NUMBER(10)           not null,
   Step_type            CHAR(1),
   OSProgram            VARCHAR2(50),
   WorkDir              VARCHAR2(64),
   ScriptName           VARCHAR2(50),
   ScriptPath           VARCHAR2(64),
   AdditionParameters   VARCHAR2(128),
   Description          VARCHAR2(64),
   Enable               CHAR(1),
   constraint PK_ETL_JOB_STEP primary key (ETL_System, ETL_Job, Step_No)
);

comment on table ETL_JOB_STEP is
'ETL事件表';

/*==============================================================*/
/* Table: ETL_Job                                               */
/*==============================================================*/
create table ETL_Job 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   ETL_Server           VARCHAR2(10)         not null,
   Description          VARCHAR2(200),
   Frequency            VARCHAR2(30),
   JobType              CHAR(1),
   Enable               CHAR(1),
   Last_StartTime       VARCHAR2(19),
   Last_EndTime         VARCHAR2(19),
   Last_JobStatus       VARCHAR2(20),
   Last_TXDate          DATE,
   Last_FileCnt         NUMBER(10),
   Last_CubeStatus      VARCHAR2(20),
   CubeFlag             CHAR(1),
   CheckFlag            CHAR(1),
   AutoOff              CHAR(1),
   CheckCalendar        CHAR(1),
   CalendarBU           VARCHAR2(15 BYTE),
   RunningScript        VARCHAR2(50),
   JobSessionID         NUMBER(10)           default 0,
   ExpectedRecord       NUMBER(10),
   CheckLastStatus      CHAR(1),
   TimeTrigger          CHAR(1),
   Job_Priority         NUMBER(10)           default 30,
   constraint PK_ETL_JOB primary key (ETL_System, ETL_Job)
);

comment on table ETL_Job is
'ETL作业表';

/*==============================================================*/
/* Table: ETL_Job_Dependency                                    */
/*==============================================================*/
create table ETL_Job_Dependency 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   Dependency_System    CHAR(3),
   Dependency_Job       VARCHAR2(50),
   Description          VARCHAR2(200),
   Enable               CHAR(1),
   constraint PK_ETL_JOB_DEPENDENCY primary key (ETL_System, ETL_Job)
);

comment on table ETL_Job_Dependency is
'ETL作业依赖表';

/*==============================================================*/
/* Table: ETL_Job_Group                                         */
/*==============================================================*/
create table ETL_Job_Group 
(
   GroupName            VARCHAR2(50)         not null,
   Description          VARCHAR2(200),
   ETL_System           CHAR(3),
   ETL_Job              VARCHAR2(50),
   AutoOnChild          CHAR(1),
   constraint PK_ETL_JOB_GROUP primary key (GroupName)
);

comment on table ETL_Job_Group is
'ETL作业组别表';

/*==============================================================*/
/* Table: ETL_Job_GroupChild                                    */
/*==============================================================*/
create table ETL_Job_GroupChild 
(
   GroupName            VARCHAR2(50)         not null,
   Description          VARCHAR2(200),
   ETL_System           CHAR(3),
   ETL_Job              VARCHAR2(50),
   Enable               CHAR(1),
   CheckFlag            CHAR(1),
   TxDate               VARCHAR2(10),
   TurnOnFlag           CHAR(1),
   constraint PK_ETL_JOB_GROUPCHILD primary key (GroupName)
);

comment on table ETL_Job_GroupChild is
'ETL作业子组表';

/*==============================================================*/
/* Table: ETL_Job_Log                                           */
/*==============================================================*/
create table ETL_Job_Log 
(
   ETL_System           CHAR(3),
   ETL_Job              VARCHAR2(50),
   JobSessionID         NUMBER(10),
   ScriptFile           VARCHAR2(60),
   TXDate               DATE,
   StartTime            VARCHAR2(19),
   EndTime              VARCHAR2(19),
   ReturnCode           NUMBER(10),
   Seconds              NUMBER(10),
   LOGContent           BLOB(1600000),
   Step_No              NUMBER(10),
   constraint PK_ETL_JOB_LOG primary key ()
);

comment on table ETL_Job_Log is
'ETL作业日志表';

/*==============================================================*/
/* Table: ETL_Job_Queue                                         */
/*==============================================================*/
create table ETL_Job_Queue 
(
   ETL_Server           VARCHAR2(10),
   SeqID                number(10),
   ETL_System           CHAR(3),
   ETL_Job              VARCHAR2(50),
   TXDate               DATE,
   RequestTime          VARCHAR2(19),
   constraint PK_ETL_JOB_QUEUE primary key ()
);

comment on table ETL_Job_Queue is
'ETL作业排队表';

/*==============================================================*/
/* Table: ETL_Job_Source                                        */
/*==============================================================*/
create table ETL_Job_Source 
(
   Source               VARCHAR2(36)         not null,
   ETL_System           CHAR(3),
   ETL_Job              VARCHAR2(50),
   Conv_File_Head       VARCHAR2(50),
   AutoFilter           CHAR(1),
   Alert                CHAR(1),
   BeforeHour           NUMBER(10),
   BeforeMin            NUMBER(10),
   OffsetDay            NUMBER(10),
   LastCount            NUMBER(10),
   constraint PK_ETL_JOB_SOURCE primary key (Source)
);

comment on table ETL_Job_Source is
'ETL作业源表';

/*==============================================================*/
/* Table: ETL_Job_Status                                        */
/*==============================================================*/
create table ETL_Job_Status 
(
   ETL_System           CHAR(3),
   ETL_Job              VARCHAR2(50),
   JobSessionID         NUMBER(10),
   TXDate               DATE,
   StartTime            VARCHAR2(19),
   EndTime              VARCHAR2(19),
   JobStatus            VARCHAR2(20),
   FileCnt              NUMBER(10),
   CubeStatus           VARCHAR2(20),
   ExpectedRecord       VARCHAR2(20),
   constraint PK_ETL_JOB_STATUS primary key ()
);

comment on table ETL_Job_Status is
'ETL作业状态表';

/*==============================================================*/
/* Table: ETL_Job_Stream                                        */
/*==============================================================*/
create table ETL_Job_Stream 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   Stream_System        CHAR(3),
   Stream_Job           VARCHAR2(50),
   Description          VARCHAR2(200),
   Enable               CHAR(1),
   constraint PK_ETL_JOB_STREAM primary key (ETL_System, ETL_Job)
);

comment on table ETL_Job_Stream is
'ETL作业下游表';

/*==============================================================*/
/* Table: ETL_Job_TimeWindow                                    */
/*==============================================================*/
create table ETL_Job_TimeWindow 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   Allow                CHAR(1),
   BeginHour            NUMBER(10),
   EndHour              NUMBER(10),
   constraint PK_ETL_JOB_TIMEWINDOW primary key (ETL_System, ETL_Job)
);

comment on table ETL_Job_TimeWindow is
'ETL作业时间触发表';

/*==============================================================*/
/* Table: ETL_Job_Trace                                         */
/*==============================================================*/
create table ETL_Job_Trace 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   TXDate               DATE                 not null,
   JobStatus            VARCHAR2(20),
   StartTime            VARCHAR2(19),
   EndTime              VARCHAR2(19),
   constraint PK_ETL_JOB_TRACE primary key (ETL_System, ETL_Job, TXDate)
);

comment on table ETL_Job_Trace is
'ETL作业运行跟踪表';

/*==============================================================*/
/* Table: ETL_Locks                                             */
/*==============================================================*/
create table ETL_Locks 
(
   ETL_Server           VARCHAR2(10),
   Start_Time           TIMESTAMP(0),
   HeartBeat            TIMESTAMP(0),
   JobCount             NUMBER(10),
   constraint PK_ETL_LOCKS primary key ()
);

comment on table ETL_Locks is
'ETL锁表';

/*==============================================================*/
/* Table: ETL_Notification                                      */
/*==============================================================*/
create table ETL_Notification 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   SeqID                NUMBER(10),
   DestType             CHAR(1),
   GroupName            VARCHAR2(15),
   UserName             VARCHAR2(15),
   Timing               CHAR(1),
   AttachLog            CHAR(1),
   Email                CHAR(1),
   ShortMessage         CHAR(1),
   MessageSubject       VARCHAR2(160),
   MessageContent       VARCHAR2(255),
   constraint PK_ETL_NOTIFICATION primary key (ETL_System, ETL_Job)
);

comment on table ETL_Notification is
'ETL作业通知表';

/*==============================================================*/
/* Table: ETL_Received_File                                     */
/*==============================================================*/
create table ETL_Received_File 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   JobSessionID         NUMBER(10),
   ReceivedFile         VARCHAR2(50)         not null,
   FileSize             NUMBER(18),
   ExpectedRecord       NUMBER(10),
   ArrivalTime          VARCHAR2(19),
   ReceivedTime         VARCHAR2(19),
   Location             VARCHAR2(128),
   Status               CHAR(1),
   constraint PK_ETL_RECEIVED_FILE primary key (ETL_System, ETL_Job, ReceivedFile)
);

comment on table ETL_Received_File is
'ETL作业接收文件表';

/*==============================================================*/
/* Table: ETL_Record_Log                                        */
/*==============================================================*/
create table ETL_Record_Log 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   JobSessionID         NUMBER(10)           not null,
   RecordTime           VARCHAR2(19),
   InsertedRecord       NUMBER(10),
   UpdatedRecord        NUMBER(10),
   DeletedRecord        NUMBER(10),
   DuplicateRecord      NUMBER(10),
   OutputRecord         NUMBER(10),
   ETRecord             NUMBER(10),
   UVRecord             NUMBER(10),
   ER1Record            NUMBER(10),
   ER2Record            NUMBER(10),
   constraint PK_ETL_RECORD_LOG primary key (ETL_System, ETL_Job, JobSessionID)
);

comment on table ETL_Record_Log is
'ETL作业记录表';

/*==============================================================*/
/* Table: ETL_RelatedJob                                        */
/*==============================================================*/
create table ETL_RelatedJob 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   RelatedSystem        CHAR(3)              not null,
   RelatedJob           VARCHAR2(50)         not null,
   CheckMode            CHAR(1)              not null,
   Description          VARCHAR2(200),
   constraint PK_ETL_RELATEDJOB primary key (ETL_System, ETL_Job, RelatedSystem, RelatedJob)
);

comment on table ETL_RelatedJob is
'ETL关联作业表';

/*==============================================================*/
/* Table: ETL_Server                                            */
/*==============================================================*/
create table ETL_Server 
(
   ETL_Server           CHAR(3)              not null,
   Description          VARCHAR2(200),
   IPAddress            VARCHAR2915),
   AgentPort            NUMBER(10),
   LiveCount            NUMBER(10),
   isPrimary            CHAR(1),
   isSMSSend            CHAR(1),
   lastRunTime          NUMBER(10),
   lastRunDate          DATE,
   constraint PK_ETL_SERVER primary key (ETL_Server)
);

comment on table ETL_Server is
'ETL系统表';

/*==============================================================*/
/* Table: ETL_Sys                                               */
/*==============================================================*/
create table ETL_Sys 
(
   ETL_System           CHAR(3)              not null,
   Description          VARCHAR2(200),
   DataKeepPeriod       NUMBER(10),
   LogKeepPeriod        NUMBER(10),
   RecordKeepPeriod     NUMBER(10),
   constraint PK_ETL_SYS primary key (ETL_System)
);

comment on table ETL_Sys is
'ETL作业系统表';

/*==============================================================*/
/* Table: ETL_TimeTrigger                                       */
/*==============================================================*/
create table ETL_TimeTrigger 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   TriggerType          CHAR(1),
   StartHour            NUMBER(10),
   StartMin             NUMBER(10),
   OffsetDay            NUMBER(10),
   constraint PK_ETL_TIMETRIGGER primary key (ETL_System, ETL_Job)
);

comment on table ETL_TimeTrigger is
'ETL时间触发表';

/*==============================================================*/
/* Table: ETL_TimeTrigger_Calendar                              */
/*==============================================================*/
create table ETL_TimeTrigger_Calendar 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   Seq                  NUMBER(10)           not null,
   YearNum              NUMBER(10),
   MonthNum             NUMBER(10),
   DayNum               NUMBER(10),
   constraint PK_ETL_TIMETRIGGER_CALENDAR primary key (ETL_System, ETL_Job, Seq)
);

comment on table ETL_TimeTrigger_Calendar is
'ETL时间触发日历表';

/*==============================================================*/
/* Table: ETL_TimeTrigger_Monthly                               */
/*==============================================================*/
create table ETL_TimeTrigger_Monthly 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   Timing               VARCHAR2(31)         not null,
   EndOfMonth           CHAR(1),
   constraint PK_ETL_TIMETRIGGER_MONTHLY primary key (ETL_System, ETL_Job, Timing)
);

comment on table ETL_TimeTrigger_Monthly is
'ETL时间触发月表';

/*==============================================================*/
/* Table: ETL_TimeTrigger_Weekly                                */
/*==============================================================*/
create table ETL_TimeTrigger_Weekly 
(
   ETL_System           CHAR(3)              not null,
   ETL_Job              VARCHAR2(50)         not null,
   Timing               VARCHAR2(7)          not null,
   constraint PK_ETL_TIMETRIGGER_WEEKLY primary key (ETL_System, ETL_Job, Timing)
);

comment on table ETL_TimeTrigger_Weekly is
'ETL时间触发周表';

/*==============================================================*/
/* Table: ETL_User                                              */
/*==============================================================*/
create table ETL_User 
(
   UserName             VARCHAR2(15)         not null,
   Description          VARCHAR2(200),
   Email                VARCHAR2(50),
   Mobile               VARCHAR2(20),
   Status               CHAR(1),
   constraint PK_ETL_USER primary key (UserName)
);

comment on table ETL_User is
'ETL用户表';

/*==============================================================*/
/* Table: ETL_UserGroup                                         */
/*==============================================================*/
create table ETL_UserGroup 
(
   GroupName            VARCHAR2(15)         not null,
   Description          VARCHAR2(200),
   constraint PK_ETL_USERGROUP primary key (GroupName)
);

comment on table ETL_UserGroup is
'ETL用户组表';

