/* =====================================================================
  prep root table
   ===================================================================== */
USE LMSMASTER
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
case when L.LoanStatus not in ('V','W','G','K') then L.OriginatedAmount else null end as OriginatedAmount, OriginationDate,
case when (ApplicationSteps not like '%R%' AND A.ApplicationSteps not like '%O%') then 'NEW' else 'RETURN' end as CustType, 
A.LPCampaign,
LP.Provider_name 
into #t1
from Application A
left join Loans L on A.Application_ID = L.ApplicationID and A.PortFolioID = L.PortFolioID
left join LeadProvider LP on A.LeadProviderID = LP.LeadProviderID
left join LMS_Logs..VW_ApplicationDump VW on A.APPGUID = VW.APPGUID
where A.ApplicationDate>= '2023-01-01' 
AND L.LoanStatus not in ('V','W','G','K') --Remember to uncomment for originated view


/* =====================================================================
  payment table prep #4
   ===================================================================== */
drop table if EXISTS #t4
select A.Application_ID, A.PortFolioID, A.LoanID, sum(case when P.PaymentStatus = 'D' then P.PaymentAmount else 0 end) as PaidOffPaymentAmount,
sum(case when P.PaymentStatus = 'R' then 1 else 0 end) as PmtReturn, count(*) as PmtCount
into #t4
from #t1 A 
inner join Payment P on A.LoanID = P.LoanID and P.PaymentMode not in ('V','P','T','H') and PaymentType in ('I','S','Z','A','X','Q','~','3') 
                        and P.InstallmentNumber >= 1 and P.PaymentDate <= getdate() and P.PaymentStatus in ('D','R') and P.PaymentAmount>2.95 and (P.PaymentAmount > P.FeeChargePaid)
where A.Originated=1
group by A.Application_ID, A.PortFolioID, A.LoanID




/* =====================================================================
  summarization table - HArvey's 
   ===================================================================== */
drop table if EXISTS #t7
;WITH Inst AS (
    SELECT
        i.LoanID,
        i.InstallmentNumber,
        i.DueDate,
        ROW_NUMBER() OVER (
            PARTITION BY i.LoanID, i.InstallmentNumber
            ORDER BY i.DueDate DESC
        ) AS rn
    FROM Installments AS i 
    WHERE iPaymentMode = 144
)
SELECT 
-- Harvey's Staging
A.LoanID,
A.OriginatedAmount,
YEAR(A.ApplicationDate)                 AS AppYear,
MONTH(A.ApplicationDate)                AS AppMonth,
DATEPART(WEEK, A.ApplicationDate)       AS AppWeek,
B.PaidOffPaymentAmount AS TotalRealizedPayin,
P.InstallmentNumber,
P.PaymentAmount AS PaidOffPaymentAmount,
-- Payment Type 
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
        END AS PaymentType, 
ROW_NUMBER() OVER (PARTITION BY A.LoanID ORDER BY P.PaymentDate ASC) AS Payment_Number,
P.PaymentStatus,
A.CustType,
A.Frequency,

P.PaymentID,
-- Application level fields
A.Application_ID,
A.PortFolioID,
A.LoanStatus,
A.NewlyScored,
A.Accepted,
A.Originated,
A.OriginationDate,
B.PmtReturn AS numOfReturn, 
B.PmtCount AS numOfPayment
INTO #t7
FROM #t1 A LEFT JOIN #t4 B ON A.Application_ID = B.Application_ID  AND A.PortFolioID = B.PortFolioID
INNER JOIN Payment AS P
    ON A.LoanID = P.LoanID
   AND P.PaymentMode NOT IN ('V','P','T','H','G')
   AND P.PaymentType IN ('I','S','Z','A','X','Q','~','3','R')    
   AND P.InstallmentNumber >= 1
   AND P.PaymentDate <= GETDATE()
   AND P.PaymentStatus IN ('D','R','A','T','P','~')
   AND P.PaymentAmount > 2.95
LEFT JOIN Inst AS I
    ON I.LoanID = P.LoanID
   AND I.InstallmentNumber = P.InstallmentNumber
   AND I.rn = 1

/* =====================================================================
  payment table prep #4a
  change the PaymentType + PaymentMode Combo to match exactly to Qlik
   ===================================================================== */
drop table if EXISTS #t4a
select A.Application_ID, A.PortFolioID, A.LoanID, sum(case when P.PaymentStatus = 'D' then P.PaymentAmount else 0 end) as PaidOffPaymentAmount,
sum(case when P.PaymentStatus = 'R' then 1 else 0 end) as PmtReturn, count(*) as PmtCount
into #t4a
from #t1 A 
inner join Payment P on A.LoanID = P.LoanID AND P.PaymentMode IN ('A','D','K','B') AND P.PaymentType IN ('Z','A','I','S','Q','X','~','3')
                        and P.InstallmentNumber >= 1 and P.PaymentStatus in ('D')
group by A.Application_ID, A.PortFolioID, A.LoanID
/* =====================================================================
  summarization table - paymentDate/TrasactionDate 
   ===================================================================== */
drop table if EXISTS #t8
;WITH Inst AS (
    SELECT
        i.LoanID,
        i.InstallmentNumber,
        i.DueDate,
        ROW_NUMBER() OVER (
            PARTITION BY i.LoanID, i.InstallmentNumber
            ORDER BY i.DueDate DESC
        ) AS rn
    FROM Installments AS i 
    WHERE iPaymentMode = 144
)
SELECT 
-- Harvey's Staging
A.LoanID,
A.OriginatedAmount,
P.PaymentDate,
P.TransactionDate,
YEAR(P.TransactionDate) AS TxYear,
MONTH(P.TransactionDate) AS TxMonth,
DATEPART(Week, P.TransactionDate) AS TxWeek,
YEAR(A.ApplicationDate)                 AS AppYear,
MONTH(A.ApplicationDate)                AS AppMonth,
DATEPART(WEEK, A.ApplicationDate)       AS AppWeek,
B.PaidOffPaymentAmount AS TotalRealizedPayin,
P.InstallmentNumber,
P.PaymentAmount AS PaidOffPaymentAmount,
-- Payment Type 
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
        END AS PaymentType, 
ROW_NUMBER() OVER (PARTITION BY A.LoanID ORDER BY P.PaymentDate ASC) AS Payment_Number,
P.PaymentStatus,
A.CustType,
A.Frequency,

P.PaymentID,
-- Application level fields
A.Application_ID,
A.PortFolioID,
A.LoanStatus,
A.NewlyScored,
A.Accepted,
A.Originated,
A.OriginationDate,
B.PmtReturn AS numOfReturn, 
B.PmtCount AS numOfPayment
INTO #t8
FROM #t1 A LEFT JOIN #t4a B ON A.Application_ID = B.Application_ID  AND A.PortFolioID = B.PortFolioID
INNER JOIN Payment AS P
    ON A.LoanID = P.LoanID
   AND P.PaymentMode NOT IN ('V','P','T','H') -- to match Qlik Finance Report
   AND P.PaymentType NOT IN ('W','9','O')    
   AND P.InstallmentNumber >= 1
   AND P.PaymentDate <= GETDATE()
   AND P.PaymentStatus IN ('D','R')
   AND P.PaymentAmount > 2.95