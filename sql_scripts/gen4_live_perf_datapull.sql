USE LMSMaster
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED 
-- 147108 rows, 010726-011226
-- 4464273, 100325-010326
-- 176128, 112625-120125, 12/1/25
drop table if EXISTS #t
select A.Application_ID, A.PortFolioID, SA.ScoredAppID AS scored_AppID, SA.ScoredPortfolioID AS scored_PortID, A.ApplicationDate, L.LoanID,
 case when A.LeadProviderID = 81 then SUBSTRING(
        A.PDLOANRCVDFROM, 
        CHARINDEX('_', A.PDLOANRCVDFROM, CHARINDEX('_', A.PDLOANRCVDFROM) + 1) + 1, 
        LEN(A.PDLOANRCVDFROM) - CHARINDEX('_', A.PDLOANRCVDFROM, CHARINDEX('_', A.PDLOANRCVDFROM) + 1)
    ) else A.PDLoanRcvdFrom end as PDLoanRcvdFrom,
CASE WHEN VW.Frequency in ('B','S') then 'B' else VW.Frequency end as Frequency, 
SA.DM_Band_Name, SA.CM_Band_Name, la.LeadProvider, la.ScoringMatrix, la.DecisionCascade, la.CM_Name, la.DM_Name,
CASE WHEN A.ApplicationSteps like '%S%' then 1 else 0 end as UnderwritingstatusScored,
CASE WHEN A.DenialCode=0 then 1 else 0 end as Accepted,
CASE WHEN A.DenialCode=0 AND NOT A.LPCampaign='RETURN' then A.OfferPrice else 0 end as BidCost,
CASE WHEN ((L.LoanStatus NOT IN ('V', 'W', 'G', 'K')) and (L.LoanStatus is not null) and (A.DenialCode=0)) THEN 1 ELSE 0 END AS Originated,
CASE WHEN ((L.LoanStatus NOT IN ('V', 'W', 'G', 'K')) and (L.LoanStatus is not null) and (A.DenialCode=0)) THEN L.OriginatedAmount ELSE 0 END AS LoansFunded, 
L.OriginationDate,  A.CustomerSSN, A.DenialCode, dc.DenialDescription, PF.FPD AS FPDAA_old
into #t from application A
LEFT JOIN QlikDB..LeadApplication la ON a.APPGUID = lA.APPGUID
LEFT JOIN LMS_Logs..VW_ApplicationDump VW on A.APPGUID = VW.APPGUID 
LEFT JOIN DenialCode dc on A.DenialCode = dc.DenialCode
LEFT JOIN [QlikDB].[dbo].[ScoredApplications] SA on A.PortFolioID = SA.PortFolioID and A.Application_ID=SA.Application_ID
LEFT JOIN Loans L on A.PortfolioID=L.PortfolioID and A.Application_ID=L.ApplicationID
LEFT JOIN DataFiles_US_Underwriting..US_Complete_Perf PF ON A.Application_ID=PF.Application_ID and A.PortFolioID=PF.PortfolioID 
where A.ApplicationSteps not like '%R%' and A.ApplicationSteps not like '%O%' 
AND (A.ApplicationDate >= '2026-03-13' AND A.ApplicationDate < '2026-03-19')
-- AND (A.ApplicationDate >= '2026-01-16' AND A.ApplicationDate <= '2026-01-26')
-- AND A.DenialCode<>100
-- AND not la.LeadProvider = 'IT Media'

-- SELECT COUNT(*) FROM application A
-- WHERE A.ApplicationSteps not like '%R%' and A.ApplicationSteps not like '%O%' 
-- AND (A.ApplicationDate >= '2026-03-13' AND A.ApplicationDate < '2026-03-18')

-- select count(*) from #t
-- 606450 rows
-- 25716 rows
DROP TABLE IF EXISTS #sr_pivot;
SELECT 
    ApplicationID,
    PortFolioID,
    MAX(CreateTime) AS Scored_ApplicationDate,
    MAX(CASE WHEN ModelID = 121 THEN Sresult END) AS IsBad_gen3,
    MAX(CASE WHEN ModelID = 122 THEN Sresult END) AS SWD_gen3,
    MAX(CASE WHEN ModelID = 123 THEN Sresult END) AS Conv_gen3,
    MAX(CASE WHEN ModelID = 124 THEN Sresult END) AS fpd_gen3
INTO #sr_pivot
FROM LMS_Logs..ScoringRResult_Archive WITH (NOLOCK)
WHERE CreateTime >= '2026-03-13' AND CreateTime < '2026-03-19'
    AND ModelID IN (121, 122, 123, 124)
  AND Sresult IS NOT NULL
GROUP BY ApplicationID, PortFolioID;
-- HAVING COUNT(DISTINCT ModelID) = 4;

-- 4464273 rows
DROP TABLE IF EXISTS #t2;
SELECT 
    A.*,
    FORMAT(A.ApplicationDate, 'yyyy-MM') AS AppMonthKey,
    SR.Scored_ApplicationDate,
    SR.IsBad_gen3, SR.SWD_gen3, SR.Conv_gen3, SR.fpd_gen3
INTO #t2
FROM #t A
LEFT JOIN #sr_pivot SR 
    ON A.scored_AppID = SR.ApplicationID 
    AND A.scored_PortID = SR.PortFolioID;


select count(*) from #t2 --#sr_pivot
where Conv_gen3 is not null

-- 1326808 rows
-- 777669 rows
-- Collect all model logs cleanly
DROP TABLE IF EXISTS #gen4_models_raw;
SELECT
    c.ApplicationID,
    c.PortfolioID,
    c.ModelName,
    TRY_CAST(JSON_VALUE(TRY_CAST(c.ModelResponse AS nvarchar(max)), '$.ModelScore') AS float) AS ModelScore,
    c.DateProcessed,
    c.IBVStatusID,
    a.CustomerSSN
INTO #gen4_models_raw
FROM  LMSMaster..Application a with (nolock) 
JOIN [LMS_Logs].[dbo].[CModelLogs] c WITH (NOLOCK) on a.PortFolioID=c.PortfolioID and a.Application_ID=c.ApplicationID
WHERE c.ModelName IN ('gen4model', 'gen4model_Conv', 'gen4model_FPD', 'gen4model_WF1', 'gen4model_Conv_v2')
  AND c.CModelLogID > 37473
  AND c.DateProcessed >= '2026-03-13' AND c.DateProcessed < '2026-03-19'
  AND ISJSON(TRY_CAST(c.ModelResponse AS nvarchar(max))) = 1;

select count(*) from #gen4_models_raw
where ModelName='gen4model_Conv_v2'
-- 4464273 rows
DROP TABLE IF EXISTS #gen4_sepoct;
SELECT
    t.Application_ID, t.PortFolioID, t.scored_AppID, t.scored_PortID, t.ApplicationDate, t.LoanID,
    t.PDLoanRcvdFrom, t.Frequency, t.UnderwritingstatusScored, t.DM_Band_Name, t.CM_Band_Name, t.LeadProvider, t.ScoringMatrix, t.DecisionCascade,
    t.CM_Name, t.DM_Name, t.Accepted, t.BidCost, t.Originated, t.LoansFunded,
    t.OriginationDate, t.CustomerSSN, t.DenialCode, t.DenialDescription,
    MAX(r.DateProcessed) AS DateProcessed,
    t.SWD_gen3, t.Conv_gen3,
    MAX(CASE WHEN r.ModelName = 'gen4model' THEN r.ModelScore END) AS gen4model,
    MAX(CASE WHEN r.ModelName = 'gen4model_Conv' THEN r.ModelScore END) AS gen4model_Conv,
    MAX(CASE WHEN r.ModelName = 'gen4model_FPD' THEN r.ModelScore END) AS gen4model_FPD,
    MAX(CASE WHEN r.ModelName = 'gen4model_isGood' THEN r.ModelScore END) AS gen4model_isGood,
    MAX(CASE WHEN r.ModelName = 'gen4model_WF1' THEN r.ModelScore END) AS gen4model_WF1,
    MAX(CASE WHEN r.ModelName = 'gen4model_Conv_v2' THEN r.ModelScore END) AS gen4model_Conv_v2
INTO #gen4_sepoct
FROM #t2 t
LEFT JOIN #gen4_models_raw r
    ON t.scored_AppID = r.ApplicationID and t.scored_PortID=r.PortfolioID
GROUP BY t.Application_ID, t.PortFolioID, t.scored_AppID, t.scored_PortID, t.ApplicationDate, t.LoanID,
    t.PDLoanRcvdFrom, t.Frequency, t.UnderwritingstatusScored, t.DM_Band_Name, t.CM_Band_Name, t.LeadProvider, t.ScoringMatrix, t.DecisionCascade,
    t.CM_Name, t.DM_Name, t.Accepted, t.BidCost, t.Originated, t.LoansFunded,
    t.OriginationDate, t.CustomerSSN, t.DenialCode, t.DenialDescription, t.SWD_gen3, t.Conv_gen3; --t.FPDAA_old, t.AppMonthKey, t.Scored_ApplicationDate, t.IsBad_gen3, t.SWD_gen3, t.Conv_gen3, t.fpd_gen3;

select count(*) from #gen4_sepoct
where gen4model_Conv_v2 is not null and Conv_gen3 is not null
and originated=1

select * from #gen4_sepoct 
where gen4model != gen4model_FPD
and gen4model is not null and gen4model_FPD is not null
order by DateProcessed desc, abs(gen4model-gen4model_FPD) desc

SELECT count(*) AS records_after_3_11_2pm
FROM #gen4_sepoct
WHERE DateProcessed > '2026-03-11 14:00:00'
and gen4model is not null and gen4model_FPD is not null;

-- Additional breakdown with scoring patterns
SELECT 
    CASE 
        WHEN gen4model_FPD IS NOT NULL AND gen4model_WF1 IS NOT NULL THEN 'Both FPD and WF1'
        WHEN gen4model_FPD IS NOT NULL THEN 'FPD Only'
        WHEN gen4model_WF1 IS NOT NULL THEN 'WF1 Only'
        ELSE 'No Scores'
    END as scoring_pattern,
    COUNT(*) as record_count
FROM #gen4_sepoct
WHERE DateProcessed > '2026-03-11 14:00:00'
GROUP BY 
    CASE 
        WHEN gen4model_FPD IS NOT NULL AND gen4model_WF1 IS NOT NULL THEN 'Both FPD and WF1'
        WHEN gen4model_FPD IS NOT NULL THEN 'FPD Only'
        WHEN gen4model_WF1 IS NOT NULL THEN 'WF1 Only'
        ELSE 'No Scores'
    END
ORDER BY record_count DESC;

-- Add FPD Performance
-- 35497 rows
DROP TABLE IF EXISTS #LoanDefault
SELECT L.LoanID, L.ApplicationID AS Application_ID, A.ApplicationDate, A.ApplicationSteps, L.PortFolioID, L.LoanStatus,
P.InstallmentNumber, I.InstallmentNumber AS Install_num_loan, P.PaymentStatus, P.PaymentType, P.PaymentMode, P.AttemptNo, P.TransactionDate, P.PaymentID,
I.InstallmentID, I.iPaymentMode, I.DueDate, I.Status, -- used to exclude pendings (code 684)
(CASE WHEN I.Status=684 THEN 1 ELSE 0 END) AS Pending
INTO #LoanDefault
FROM LMSMaster..Loans L
LEFT JOIN LMSMaster..Payment P ON P.LoanID = L.LoanID
LEFT JOIN LMSMaster..Installments I ON I.InstallmentID = P.InstallmentID
LEFT JOIN LMSMaster..Application A ON A.PortfolioID=L.PortfolioID AND A.Application_ID = L.ApplicationID
-- WHERE A.ApplicationDate >= '2025-07-01' AND A.ApplicationDate <= '2025-11-10' 
WHERE A.ApplicationDate >= '2026-03-11' AND A.ApplicationDate <= '2026-03-12'
AND I.InstallmentNumber = 1

DROP TABLE IF EXISTS #LoanDefault_Flag
SELECT 
    L.*,
    CASE 
        WHEN L.PaymentStatus = 'R'
             AND L.PaymentType IN ('I','S','A')
             AND L.PaymentMode IN ('A','B','D')
             AND L.DueDate <= CAST(GETDATE() AS date)
             AND NOT EXISTS (
                 SELECT 1
                 FROM #LoanDefault ld
                 WHERE ld.InstallmentID = L.InstallmentID
                   AND ld.PaymentStatus = 'D'
                   AND ld.PaymentType NOT IN ('3','~','Q')
                   AND ld.PaymentMode IN ('A','D','B')
                   AND CONVERT(date, ld.TransactionDate) = CONVERT(date, L.DueDate)
             )
        THEN 1 ELSE 0 END AS is_FPDFA,
    CASE 
        WHEN L.LoanStatus NOT IN ('V','W','G','K')
             AND NOT (
                 L.iPaymentMode = 144 
                 AND L.Pending = 1
                 AND L.DueDate >= CAST(GETDATE() AS date)
             )
        THEN 1 ELSE 0 
    END AS is_loan_first_install
INTO #LoanDefault_Flag
FROM #LoanDefault L;

DROP TABLE IF EXISTS #LoanDefault_Dedup;
WITH dedup AS (
    SELECT LoanID, Application_ID, ApplicationDate, ApplicationSteps, PortfolioID, LoanStatus,
           InstallmentNumber, PaymentStatus, PaymentType, PaymentMode, AttemptNo, TransactionDate,
           PaymentID, InstallmentID, iPaymentMode, DueDate, Status, Pending, is_FPDFA, is_loan_first_install,
           ROW_NUMBER() OVER (PARTITION BY Application_ID, PortfolioID ORDER BY is_FPDFA DESC) AS rn
    FROM #LoanDefault_Flag
)
SELECT LoanID, Application_ID, ApplicationDate, ApplicationSteps, PortfolioID, LoanStatus,
       InstallmentNumber, PaymentStatus, PaymentType, PaymentMode, AttemptNo, TransactionDate,
       PaymentID, InstallmentID, iPaymentMode, DueDate, Status, Pending, is_FPDFA, is_loan_first_install
INTO #LoanDefault_Dedup
FROM dedup
WHERE rn = 1;

-- Append FPD flags
DROP TABLE IF EXISTS #gen4_fpd_perf;
SELECT t.*, ld.DueDate AS FirstInstall_DueDate, ld.is_FPDFA, ld.is_loan_first_install 
INTO #gen4_fpd_perf
FROM #gen4_sepoct t 
LEFT JOIN #LoanDefault_Dedup ld ON t.LoanID = ld.LoanID;

-- Step: 
    -- SUM(CASE 
    --         WHEN PaymentStatus = 'D' 
    --              AND PaymentType IN ('Z','A','I','S','Q','X','~','3')
    --              AND PaymentMode IN ('B','A','K','D')
    --         THEN PaymentAmount 
    --         ELSE 0 
    --     END) * 1.0
    -- /
    -- NULLIF(SUM(CASE 
    --                WHEN PaymentType = 'O'
    --                     AND PaymentStatus = 'F'
    --                     AND LoanStatus NOT IN ('V','W','G','K')
    --                THEN PaymentAmount
    --                ELSE 0
    --            END), 0) AS Payin


-- Check performance to validate data pull
-- FPDFA
SELECT SUM(is_FPDFA)*100.0/SUM(is_loan_first_install) AS FPDFA_new
-- ,SUM(FPDAA_old)*100.0/SUM(CASE WHEN FPDAA_old IS NOT NULL THEN 1 ELSE 0 END) AS FPDAA_old
FROM #gen4_fpd_perf;

-- Number FPD, originated, and accept from output table 2 (with fpd performance; use for analysis)
SELECT -- SUM(FPDAA_old) FPDAA_old_count,
SUM(is_FPDFA) AS FPDFA_new_count, SUM(is_loan_first_install) AS first_install_dued,
    SUM(Originated) Originated_count, SUM(Accepted) Accepted_count, 
    SUM(Accepted)*100.0/COUNT(*) AS Accept_rate, 
    SUM(Originated)*100.0/SUM(Accepted) AS Origination_rate, COUNT(*) As Scored_count 
FROM #gen4_fpd_perf
WHERE
NOT (gen4model is null and gen4model_conv is null and gen4model_FPD is null and gen4model_isGood is null and gen4model_WF1 is null);

-- Number originated and accept from application table (initial joint step)
SELECT 
    SUM(Originated) Originated_count, SUM(Accepted) Accepted_count, 
    SUM(Accepted)*100.0/COUNT(*) AS Accept_rate, 
    SUM(Originated)*100.0/SUM(Accepted) AS Origination_rate, COUNT(*) As count 
FROM #t;
-- Number originated and accept from output table 1 (use for analysis)
SELECT 
    SUM(Originated) Originated_count, SUM(Accepted) Accepted_count, 
    SUM(Accepted)*100.0/COUNT(*) AS Accept_rate, 
    SUM(Originated)*100.0/SUM(Accepted) AS Origination_rate, COUNT(*) As count 
FROM #gen4_sepoct;
-- Number originated and accept from output table 2 (with fpd performance; use for analysis)
SELECT 
    SUM(Originated) Originated_count, SUM(Accepted) Accepted_count, 
    SUM(Accepted)*100.0/COUNT(*) AS Accept_rate, 
    SUM(Originated)*100.0/SUM(Accepted) AS Origination_rate, COUNT(*) As count 
FROM #gen4_fpd_perf;
-- Number originated and accept from output table 1 (with at least one valid bureau score; use for analysis)
SELECT 
    SUM(Originated) Originated_count, SUM(Accepted) Accepted_count, COUNT(*) Volume,
    SUM(Accepted)*100.0/COUNT(*) AS Accept_rate,  
    SUM(Originated)*100.0/SUM(Accepted) AS Origination_rate, COUNT(*) As count 
FROM #gen4_sepoct
WHERE NOT (
        -- ISBad_gen3 is null and Conv_gen3 is null and fpd_gen3 is null and 
        gen4model_FPD is null and gen4model_WF1 is null);

select * FROM #gen4_sepoct
WHERE accepted=1 and gen4model_WF1<480


SELECT * FROM #gen4_sepoct
WHERE NOT (
        -- ISBad_gen3 is null and Conv_gen3 is null and fpd_gen3 is null and 
        gen4model_FPD is null and gen4model_WF1 is null)
AND NOT DateProcessed is null;

-- check null DM/CM Band
SELECT SUM(CASE WHEN CM_Band_Name is not null then 1 else 0 end) AS valid_DM FROM #t;
SELECT SUM(CASE WHEN CM_Band_Name is not null then 1 else 0 end) AS valid_DM FROM #gen4_sepoct;
SELECT SUM(CASE WHEN CM_Band_Name is not null then 1 else 0 end) AS valid_DM FROM #gen4_sepoct
WHERE NOT (-- SWD_gen3 is null and ISBad_gen3 is null and Conv_gen3 is null and fpd_gen3 is null and 
            gen4model_conv is null and gen4model_FPD is null and gen4model_isGood is null and gen4model_WF1 is null);
SELECT SUM(CASE WHEN CM_Band_Name is not null then 1 else 0 end) AS valid_DM FROM #gen4_sepoct
WHERE NOT (gen4model is null and gen4model_conv is null and gen4model_FPD is null and gen4model_isGood is null and gen4model_WF1 is null);



SELECT * FROM #gen4_fpd_perf
WHERE NOT (gen4model_FPD is null and gen4model_WF1 is null)


SELECT * FROM #gen4_fpd_perf
WHERE is_loan_first_install=1
AND gen4model_FPD is not null AND gen4model_WF1 is not null

SELECT 
SUM(is_FPDFA) as count_FPDFA, SUM(is_loan_first_install) AS count_first_install,
SUM(is_FPDFA)*100.0/SUM(is_loan_first_install) AS FPDFA_new
FROM #gen4_fpd_perf
WHERE is_loan_first_install=1 
AND ApplicationDate >= '2025-12-10'
AND (gen4model_WF1 is not null or gen4model_FPD is not null);



SELECT * FROM 
#gen4_fpd_perf
WHERE is_loan_first_install=1 
AND ApplicationDate >= '2025-12-10'
AND (gen4model_WF1 is not null and gen4model_FPD is not null);


SELECT Distinct LeadProvider from #gen4_sepoct


SELECT ScoringMatrix, SUM(is_FPDFA) AS NumFirstDefault,
SUM(is_loan_first_install) AS FirstInstallDue,
SUM(is_FPDFA)*100.0/SUM(is_loan_first_install) AS FPDFA_new
FROM #gen4_fpd_perf
WHERE is_loan_first_install=1
GROUP BY ScoringMatrix


SELECT SUM(UnderwritingstatusScored) as total_records,
SUM(CASE WHEN gen4model_Conv_v2 is not null THEN 1 ELSE 0 END) AS count_gen4model_Conv_v2,
SUM(CASE WHEN gen4model_Conv_v2 is not null THEN 1 ELSE 0 END)*100.0/NULLIF(SUM(UnderwritingstatusScored), 0) AS percent_gen4model_Conv_v2
FROM #gen4_sepoct

SELECT
COUNT(*) AS TotalRecords,
ROUND(CAST(SUM(Accepted) AS FLOAT) / NULLIF(COUNT(*), 0), 4)*100 as AcceptRate,
SUM(Originated) ConversionCount,
ROUND(CAST(SUM(Originated) AS FLOAT) / NULLIF(SUM(Accepted), 0), 4)*100 as ConvRate 
FROM #gen4_sepoct
WHERE gen4model_Conv_v2 is not null

SELECT * FROM #gen4_sepoct
WHERE Conv_gen3 is not null -- gen4model_Conv_v2 is not null
and Accepted=1