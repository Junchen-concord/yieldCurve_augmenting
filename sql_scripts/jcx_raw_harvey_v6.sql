/* =====================================================================
   SECTION 1: Base Application Table Starting appYear 2023
   ===================================================================== */
use LMSMaster;
drop table if EXISTS #t1
select A.Application_ID, A.PortfolioID, A.CustomerID, A.ApplicationDate, 
YEAR(A.ApplicationDate)                 AS AppYear,
MONTH(A.ApplicationDate)                AS AppMonth,
DATEPART(WEEK, A.ApplicationDate)       AS AppWeek,
L.LoanStatus,
case when ApplicationSteps like '%S%' then 1 else 0 end as NewlyScored,
CASE WHEN A.DenialCode=0 then 1 else 0 end as Accepted,
case when A.DenialCode=0 then A.LeadPurchasePrice else 0 end as LeadPurchasePrice,
case when L.LoanStatus not in ('V','W','G','K') then 1 else 0 end as Originated,
L.LoanID, datediff(year, VW.DOB, A.ApplicationDate) as Age, VW.Frequency,
case when L.LoanStatus not in ('V','W','G','K') then L.OriginatedAmount else null end as OriginatedAmount, CAST(OriginationDate AS date) AS OriginationDate,
case when (ApplicationSteps not like '%R%' AND A.ApplicationSteps not like '%O%') then 'NEW' else 'RETURN' end as CustType, 
A.LPCampaign,
LP.Provider_name 
into #t1
from Application A
left join Loans L on A.Application_ID = L.ApplicationID and A.PortFolioID = L.PortFolioID
left join LeadProvider LP on A.LeadProviderID = LP.LeadProviderID
left join LMS_Logs..VW_ApplicationDump VW on A.APPGUID = VW.APPGUID
where A.ApplicationDate>= '2023-01-01' 
AND L.LoanStatus not in ('V','W','G','K') 

/* =====================================================================
   SECTION 2: Payment Table
   ===================================================================== */
drop table if EXISTS #t4
select A.Application_ID, A.PortFolioID, A.LoanID, sum(case when P.PaymentStatus = 'D' then P.PaymentAmount else 0 end) as TotalrealizedPayment
into #t4
from #t1 A 
inner join Payment P on A.LoanID = P.LoanID and P.PaymentMode IN ('A','D','K','B') AND P.PaymentType IN ('Z','A','I','S','Q','X','~','3')
                        and P.InstallmentNumber >= 1 and P.PaymentStatus in ('D')
group by A.Application_ID, A.PortFolioID, A.LoanID

/* =====================================================================
   SECTION 3: Installment Level Payment Table
   ===================================================================== */
drop table if EXISTS #t5
select A.Application_ID, A.PortFolioID, A.LoanID, P.InstallmentNumber, 
SUM(case when P.PaymentStatus = 'D' then P.PaymentAmount else 0 end) as InstallrealizedPayment,
MAX(CASE WHEN P.PaymentStatus = 'D' AND PaymentAmount > 0 AND P.PaymentType = '3' THEN 1 ELSE 0 END) AS ThirdPartyCollected,
MAX(CASE WHEN P.PaymentStatus = 'D' AND PaymentAmount > 0 AND P.PaymentType = 'A' THEN 1 ELSE 0 END) AS PartialCollected,
MAX(CASE WHEN P.PaymentStatus = 'D' AND PaymentAmount > 0 AND P.PaymentType = 'I' THEN 1 ELSE 0 END) AS InstallCollected,
MAX(CASE WHEN P.PaymentStatus = 'D' AND PaymentAmount > 0 AND P.PaymentType = 'Z' THEN 1 ELSE 0 END) AS EarlyCollected
into #t5
from #t1 A 
inner join Payment P on A.LoanID = P.LoanID and P.PaymentMode IN ('A','D','K','B') AND P.PaymentType IN ('Z','A','I','S','Q','X','~','3')
                        and P.InstallmentNumber >= 1 and P.PaymentStatus in ('D')
group by A.Application_ID, A.PortFolioID, A.LoanID, P.InstallmentNumber

/* =====================================================================
   SECTION 2: inline summary table method #t17
   ===================================================================== */
drop table if EXISTS #t17
SELECT 
A.LoanID,
-- customer information
YEAR(A.ApplicationDate)                 AS AppYear,
MONTH(A.ApplicationDate)                AS AppMonth,
DATEPART(WEEK, A.ApplicationDate)       AS AppWeek,
A.CustType,
A.Frequency,
A.Application_ID,
A.PortFolioID,
A.LoanStatus,
A.NewlyScored,
A.Accepted,
A.Originated,
A.OriginationDate,

--payment information 
A.OriginatedAmount,
I.InstallmentNumber,
COALESCE(t5.InstallrealizedPayment, 0)  AS PaidOffThisInstall,
t4.TotalrealizedPayment AS TotalRealizedPayin,
I.[Status] AS InstallStatusCode,
DP.[Description] AS InstallmentStatus,
/* *************** PaidOff Flag *************** */
CASE
    WHEN I.[Status] = 111
    AND (
        -- Original: last active install before voided 115 cleanup rows
        (
            MIN(I.[Status]) OVER (PARTITION BY I.LoanID ORDER BY I.InstallmentNumber ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) = 115
            AND MAX(I.[Status]) OVER (PARTITION BY I.LoanID ORDER BY I.InstallmentNumber ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) = 115
        )
        OR
        -- New: all installments are 111, no rows follow — this is the final installment
        MIN(I.[Status]) OVER (PARTITION BY I.LoanID ORDER BY I.InstallmentNumber ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) IS NULL
    )
    THEN 1
    ELSE 0
END AS LoanPaidOff,
CASE 
    --WHEN I.[Status] = 111  AND A.LoanStatus = 'R'                                                 THEN 1  -- system verdict first
    WHEN I.[Status] = 111                                                   THEN 0  -- paid off
    WHEN I.[Status] IN (115, 779) AND t5.InstallrealizedPayment > 0        THEN 0  -- voided/returned with money
    WHEN t5.PartialCollected = 1                                            THEN 0  -- partial payment collected
    WHEN I.[Status] = 684                                                   THEN 0  -- pending
    ELSE 1 
END AS isLoanDefault,
-- Add a separate recent loan flag:
CASE WHEN I.[Status] = 684 THEN 1 ELSE 0 END AS isRecentInstall,
CASE 
    WHEN I.[Status] NOT IN (111, 779, 684) THEN 1
    WHEN I.[Status] = 115 AND LAG(I.[Status]) OVER (PARTITION BY I.LoanID 
                                   ORDER BY I.InstallmentNumber) <> 111             THEN 1  
    ELSE 0 
END AS isInstallDefault,
t5.ThirdPartyCollected,
t5.PartialCollected,
t5.InstallCollected,
t5.EarlyCollected,
CAST(I.DueDate AS DATE) AS InstallDueDate
INTO #t17
FROM #t1 A 
INNER JOIN LMSMaster..Installments I ON I.LoanID = A.LoanID AND I.iPaymentMode = 144
INNER JOIN LMSMaster..DropDownCodes DP ON DP.DropDownCodeID = I.[Status]
LEFT JOIN #t4 t4 ON t4.LoanID = A.LoanID
                 AND t4.Application_ID = A.Application_ID
                 AND t4.PortFolioID = A.PortFolioID
LEFT JOIN #t5 t5 ON t5.LoanID = A.LoanID
                 AND t5.Application_ID = A.Application_ID
                 AND t5.PortFolioID = A.PortFolioID
                 AND t5.InstallmentNumber = I.InstallmentNumber


/* ---------------------------------------------------------------------
   SECTION trim: seperate good customers from the aggregated
   --------------------------------------------------------------------- */
DROP TABLE IF EXISTS #t17_final
SELECT *,
    COUNT(*) OVER (PARTITION BY LoanID) AS TotalInstallsNumber
INTO #t17_final
FROM #t17 t17
WHERE (
    -- Rule 1: keep rows up to and including first terminal event
    t17.InstallmentNumber <= (
        SELECT MIN(sub.InstallmentNumber)
        FROM #t17 sub
        WHERE sub.LoanID = t17.LoanID
          AND (sub.LoanPaidOff = 1 OR sub.isLoanDefault = 1)
    )
)
OR (
    -- Rule 2: no terminal event exists — keep all rows except recent loan duplicates
    NOT EXISTS (
        SELECT 1 FROM #t17 sub
        WHERE sub.LoanID = t17.LoanID
          AND (sub.LoanPaidOff = 1 OR sub.isLoanDefault = 1)
    )
    AND (t17.isRecentInstall = 0 OR t17.InstallmentNumber = 1)
)


DROP TABLE IF EXISTS #monthly_summary
SELECT
    AppYear,
    AppMonth,
    COUNT(DISTINCT LoanID)                                                              AS CohortLoans,

    -- FPD
    SUM(CASE WHEN InstallmentNumber = 1 AND TotalInstallsNumber >= 1 THEN 1 ELSE 0 END)                          AS FPD_Denom,
    SUM(CASE WHEN InstallmentNumber = 1 AND TotalInstallsNumber >= 1 AND isInstallDefault = 1 THEN 1 ELSE 0 END) AS FPD_Num,
    CAST(SUM(CASE WHEN InstallmentNumber = 1 AND TotalInstallsNumber >= 1 AND isInstallDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN InstallmentNumber = 1 AND TotalInstallsNumber >= 1 THEN 1 ELSE 0 END), 0)         AS FPD_Rate,

    -- SPD
    SUM(CASE WHEN InstallmentNumber = 2 AND TotalInstallsNumber >= 2 THEN 1 ELSE 0 END)                          AS SPD_Denom,
    SUM(CASE WHEN InstallmentNumber = 2 AND TotalInstallsNumber >= 2 AND isInstallDefault = 1 THEN 1 ELSE 0 END) AS SPD_Num,
    CAST(SUM(CASE WHEN InstallmentNumber = 2 AND TotalInstallsNumber >= 2 AND isInstallDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN InstallmentNumber = 2 AND TotalInstallsNumber >= 2 THEN 1 ELSE 0 END), 0)         AS SPD_Rate,

    -- TPD
    SUM(CASE WHEN InstallmentNumber = 3 AND TotalInstallsNumber >= 3 THEN 1 ELSE 0 END)                          AS TPD_Denom,
    SUM(CASE WHEN InstallmentNumber = 3 AND TotalInstallsNumber >= 3 AND isInstallDefault = 1 THEN 1 ELSE 0 END) AS TPD_Num,
    CAST(SUM(CASE WHEN InstallmentNumber = 3 AND TotalInstallsNumber >= 3 AND isInstallDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN InstallmentNumber = 3 AND TotalInstallsNumber >= 3 THEN 1 ELSE 0 END), 0)         AS TPD_Rate,

    -- 4PD
    SUM(CASE WHEN InstallmentNumber = 4 AND TotalInstallsNumber >= 4 THEN 1 ELSE 0 END)                          AS PD4_Denom,
    SUM(CASE WHEN InstallmentNumber = 4 AND TotalInstallsNumber >= 4 AND isInstallDefault = 1 THEN 1 ELSE 0 END) AS PD4_Num,
    CAST(SUM(CASE WHEN InstallmentNumber = 4 AND TotalInstallsNumber >= 4 AND isInstallDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN InstallmentNumber = 4 AND TotalInstallsNumber >= 4 THEN 1 ELSE 0 END), 0)         AS PD4_Rate,

    -- 5PD
    SUM(CASE WHEN InstallmentNumber = 5 AND TotalInstallsNumber >= 5 THEN 1 ELSE 0 END)                          AS PD5_Denom,
    SUM(CASE WHEN InstallmentNumber = 5 AND TotalInstallsNumber >= 5 AND isInstallDefault = 1 THEN 1 ELSE 0 END) AS PD5_Num,
    CAST(SUM(CASE WHEN InstallmentNumber = 5 AND TotalInstallsNumber >= 5 AND isInstallDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN InstallmentNumber = 5 AND TotalInstallsNumber >= 5 THEN 1 ELSE 0 END), 0)         AS PD5_Rate
INTO #monthly_summary
FROM #t17_final
GROUP BY AppYear, AppMonth
ORDER BY AppYear, AppMonth