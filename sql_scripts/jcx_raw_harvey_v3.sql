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




drop table if EXISTS #t7
SELECT 
-- Columns organized to match output order
A.LoanID,
A.OriginatedAmount,
YEAR(A.ApplicationDate)                 AS AppYear,
MONTH(A.ApplicationDate)                AS AppMonth,
DATEPART(WEEK, A.ApplicationDate)       AS AppWeek,
I.InstallmentNumber,
CASE WHEN P.PaymentMode IN ('A','D','K','B') AND P.PaymentType IN ('Z','A','I','S','Q','X','~','3')
                        and P.InstallmentNumber >= 1 AND P.PaymentStatus = 'D' THEN P.PaymentAmount ELSE 0 END AS CollectedPaymentAmount,
CASE P.PaymentType
    WHEN 'I' THEN 'Installment Pmt'
    WHEN 'X' THEN 'Reset Pmt'
    WHEN '~' THEN 'Arr Pmt'
    WHEN 'A' THEN 'Partial Pmt'
    WHEN 'Q' THEN 'Delinquent Pmt'
    WHEN '3' THEN '3rd Party Pmt'
    WHEN 'R' THEN 'Amt rolled over for renewal'
    WHEN 'Z' THEN 'Early Pmt' 
    ELSE P.PaymentType 
END                                     AS PaymentType, 
ROW_NUMBER() OVER (PARTITION BY A.LoanID ORDER BY P.PaymentDate ASC) AS Payment_Number,
P.PaymentStatus,
A.CustType,
A.Frequency,
P.PaymentID,
A.Application_ID,
A.PortFolioID,
A.LoanStatus,
A.NewlyScored,
A.Accepted,
A.Originated,
A.OriginationDate,
SUM(CASE WHEN P.PaymentStatus = 'R' THEN 1 ELSE 0 END) 
    OVER (PARTITION BY A.LoanID)       AS numOfReturn,
COUNT(*) OVER (PARTITION BY A.LoanID)  AS numOfPayment,
CAST(P.PaymentDate AS DATE) AS PaymentDate,
YEAR(P.PaymentDate)                     AS PmtYear,
MONTH(P.PaymentDate)                    AS PmtMonth,
DATEPART(WEEK, P.PaymentDate)           AS PmtWeek,
I.[Status] AS InstallStatusCode,
DP.[Description] AS InstallmentStatus,
CAST(I.DueDate AS DATE) AS InstallDueDate
INTO #t7
FROM #t1 A 
INNER JOIN Payment AS P ON A.LoanID = P.LoanID
INNER JOIN LMSMaster..Installments I ON I.InstallmentID = P.InstallmentID AND I.iPaymentMode = 144
INNER JOIN LMSMaster..DropDownCodes DP ON DP.DropDownCodeID = I.[Status] 



/* =====================================================================
   EXTRA SECTION:  Ununsed Payin Calculation from the team as 
   reference. The actual data extract ends at #t7 data extract.
   ===================================================================== */

-- now merge first payment default and payin
drop table if exists #payments
SELECT Perf.LoanID, Perf.Application_ID, Perf.PortFolioID, Perf.ApplicationDate, Provider_name, Perf.Originated, Perf.OriginationDate, LoanStatus, Perf.LoansFunded,
P.InstallmentNumber, P.PaymentStatus, P.PaymentType, P.PaymentMode, P.AttemptNo, P.TransactionDate, P.PaymentID,PaymentAmount,P.PaymentDate,
I.InstallmentID, I.iPaymentMode, I.DueDate, I.Status, -- used to exclude pendings (code 684)
(CASE WHEN I.Status=684 THEN 1 ELSE 0 END) AS Pending
INTO #payments
FROM #t1 Perf
LEFT JOIN LMSMaster..Payment P ON P.LoanID = Perf.LoanID
LEFT JOIN LMSMaster..Installments I ON I.InstallmentID = P.InstallmentID

DROP TABLE IF EXISTS #payin
SELECT 
    t.LoanID,
    t.Application_ID,
    t.PortFolioID,
    t.ApplicationDate,
    t.LoansFunded,
        SUM(CASE 
            WHEN PaymentStatus = 'D' 
                 AND PaymentType IN ('Z','A','I','S','Q','X','~','3')
                 AND PaymentMode IN ('B','A','K','D')
                 AND TransactionDate <= DATEADD(day, 90, OriginationDate)
            THEN PaymentAmount 
            ELSE 0 
        END) * 1.0
    /
    NULLIF(t.LoansFunded, 0) AS Payin_90days,
    SUM(CASE 
            WHEN PaymentStatus = 'D' 
                 AND PaymentType IN ('Z','A','I','S','Q','X','~','3')
                 AND PaymentMode IN ('B','A','K','D')
                 AND TransactionDate <= DATEADD(day, 120, OriginationDate)
            THEN PaymentAmount 
            ELSE 0 
        END) * 1.0
    /
    NULLIF(t.LoansFunded, 0) AS Payin_120days
INTO #payin
FROM #payments t
GROUP BY t.LoanID, t.Application_ID, t.PortFolioID, t.ApplicationDate, t.LoansFunded;
