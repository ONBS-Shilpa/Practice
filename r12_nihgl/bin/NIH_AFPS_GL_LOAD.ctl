--============================================================================
-- File Name            : NIH_AFPS_GL_ACCT_LOAD.ctl
-- Revision             : 1.0
-- Date                 : 22-NOV-2010
-- Original Author      : Anand Chaturvedi
--
-- Description          : SQL*Loader script to load AFPS GL Data into staging tables.
--
-- Change History
-- Version      When            Who                What
-- =========    ==============  ================= ============================
-- 1.0          20-NOV-2010     Anand Chaturvedi     Initial Creation
-- 2.0          18-SEP-2021     Krishna Aravapalli    Oc4 to 5 and change fields after that accordingly-CR42505
--============================================================================
--OPTIONS(BINDSIZE=10000000,READSIZE=5120000,ROWS=15000)
OPTIONS (BINDSIZE 512000,ROWS= 10000)
LOAD DATA
APPEND
INTO TABLE NIHGL_AFPS_ACCT_HDRS_STG_TBL
WHEN (1:1 = '0')
TRAILING NULLCOLS
(
 RECORD_ID                    "NIHGL_AFPS_ACCT_HDRS_STG_S.NEXTVAL"
,AGENCY_CODE                  POSITION(2:2)
,PAYROLL_CATEGORY             POSITION(3:5)
,PAY_PERIOD                   POSITION(6:7)
,PAYROLL_TYPE                 POSITION(8:8)
,PAY_PERIOD_END_DATE          POSITION(9:16)
,FILE_IDENTIFIER              POSITION(17:20)
,RECORD_LENGTH                POSITION(21:24)
,CONTROL_NUMBER               POSITION(25:30)
,ORG_ID                       "NIH_COMMON_PK.GET_ORG_ID('NIH-OU')"
,SET_OF_BOOKS_ID              "NIH_COMMON_PK.GET_SOB_ID('NIH')"
,RECORD_STATUS                CONSTANT "N"
,FILE_LOAD_DATE               SYSDATE
,FILE_NAME                    CONSTANT "<<NIH_FILE>>"
,REQUEST_ID                   CONSTANT "<<NIH_REQID>>"
,CREATED_BY                   CONSTANT "<<NIH_USERID>>"
,CREATION_DATE                SYSDATE
,LAST_UPDATED_BY              CONSTANT "<<NIH_USERID>>"
,LAST_UPDATE_DATE             SYSDATE
)
INTO TABLE NIHGL_AFPS_ACCT_BTCHS_STG_TBL
WHEN (1:1 = '1')
TRAILING NULLCOLS
(
 RECORD_ID                    "NIHGL_AFPS_ACCT_BTCHS_STG_S.NEXTVAL"
,ACCOUNTING_DATE              POSITION(7:14)
,BATCH_NUMBER                 POSITION(15:16)
,CONTROL_NUMBER               POSITION(17:22)
,AGENCY_CODE                  POSITION(42:42)
,ACCOUNTING_POINT             POSITION(43:44)
,ORG_ID                       "NIH_COMMON_PK.GET_ORG_ID('NIH-OU')"
,SET_OF_BOOKS_ID              "NIH_COMMON_PK.GET_SOB_ID('NIH')"
,RECORD_STATUS                CONSTANT "N"
,FILE_LOAD_DATE               SYSDATE
,FILE_NAME                    CONSTANT "<<NIH_FILE>>"
,REQUEST_ID                   CONSTANT "<<NIH_REQID>>"
,CREATED_BY                   CONSTANT "<<NIH_USERID>>"
,CREATION_DATE                SYSDATE
,LAST_UPDATED_BY              CONSTANT "<<NIH_USERID>>"
,LAST_UPDATE_DATE             SYSDATE
)
INTO TABLE NIHGL_AFPS_ACCT_DTLS_STG_TBL
WHEN (1:1 = '2')
TRAILING NULLCOLS
(
 RECORD_ID                    "NIHGL_AFPS_ACCT_DTLS_STG_S.NEXTVAL"
,ACCOUNTING_DATE              POSITION(2:9)
,TCODE                        POSITION(10:12)
,REVERSE_CODE                 POSITION(13:13)
,MODIFIER_CODE                POSITION(14:14)
,DOCUMENT_REF                 POSITION(15:17)
,DOCUMENT_NUMBER              POSITION(18:37)
,OTH_DOCUMENT_REF             POSITION(38:40) 
,OTH_DOCUMENT_NUMBER          POSITION(41:60) 
,GEO_CODE                     POSITION(61:61)
,CAN_FY                       POSITION(62:65)
,AGENCY_CODE                  POSITION(66:66)
,ORG_ID                      "NIH_COMMON_PK.GET_ORG_ID('NIH-OU')"
,SET_OF_BOOKS_ID             "NIH_COMMON_PK.GET_SOB_ID('NIH')"
,ACCOUNTING_POINT             POSITION(67:68)
,CAN                          POSITION(66:72)
,OBJECT_CLASS                 POSITION(75:79)        --CR 42505
,AMOUNT                       POSITION(80:94)
,PRIMARY_EIN                  POSITION(95:106)
,SECONDARY_EIN                POSITION(107:118)
,CONTROL_NUMBER               POSITION(119:124)
,SCHEDULE_NUMBER              POSITION(119:124)
,AFPS_GL_DR_ACCT              POSITION(132:135)
,AFPS_GL_CR_ACCT              POSITION(136:139)
,PERMANENT_CODE               POSITION(140:140)
,FUND_CODE                    POSITION(141:141)
,GL_SUB_ACCT_CODE             POSITION(142:142)
,ACCT_LAW_CODE                POSITION(143:143)
,AWARD_BEGIN_DATE             POSITION(144:151)
,AWARD_END_DATE               POSITION(152:159)
,RESERVED_CODE                POSITION(160:160)
,RESERVED_FUTURE              POSITION(161:161)
,RECORD_STATUS                CONSTANT "N"
,FILE_LOAD_DATE               SYSDATE
,FILE_NAME                    CONSTANT "<<NIH_FILE>>"
,REQUEST_ID                   CONSTANT "<<NIH_REQID>>"
,CREATED_BY                   CONSTANT "<<NIH_USERID>>"
,CREATION_DATE                SYSDATE
,LAST_UPDATED_BY              CONSTANT "<<NIH_USERID>>"
,LAST_UPDATE_DATE             SYSDATE
)
INTO TABLE NIHGL_AFPS_ACCT_BTCHS_STG_TBL
WHEN (1:1 = '4')
TRAILING NULLCOLS
(
 RECORD_ID                    "NIHGL_AFPS_ACCT_BTCHS_STG_S.NEXTVAL"
,ACCOUNTING_DATE              POSITION(7:14)
,BATCH_NUMBER                 POSITION(15:16)
,LINES_TOTAL                  POSITION(17:24)
,AGENCY_CODE                  POSITION(42:42)
,ACCOUNTING_POINT             POSITION(43:44)
,LINES_TOTAL_AMOUNT           POSITION(54:70)
,ORG_ID                      "NIH_COMMON_PK.GET_ORG_ID('NIH-OU')"
,SET_OF_BOOKS_ID             "NIH_COMMON_PK.GET_SOB_ID('NIH')"
,RECORD_STATUS                CONSTANT "N"
,FILE_LOAD_DATE               SYSDATE
,FILE_NAME                    CONSTANT "<<NIH_FILE>>"
,REQUEST_ID                   CONSTANT "<<NIH_REQID>>"
,CREATED_BY                   CONSTANT "<<NIH_USERID>>"
,CREATION_DATE                SYSDATE
,LAST_UPDATED_BY              CONSTANT "<<NIH_USERID>>"
,LAST_UPDATE_DATE             SYSDATE
)