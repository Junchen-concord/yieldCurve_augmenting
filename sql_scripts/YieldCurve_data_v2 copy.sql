USE LMSMASTER

drop table if EXISTS #t1
select A.Application_ID, A.PortfolioID, A.CustomerID, A.ApplicationDate,
case when ApplicationSteps like '%S%' then 1 else 0 end as NewlyScored,
case when A.ApplicationStatus in ('A','P') then 1 else 0 end as Accepted,
case when A.ApplicationStatus in ('A','P') then A.LeadPurchasePrice else 0 end as LeadPurchasePrice,
case when L.LoanStatus not in ('V','W','G','K') then 1 else 0 end as Originated,
L.LoanID, datediff(year, VW.DOB, A.ApplicationDate) as Age,
case when VW.Frequency in ('B','S') then 'B' when VW.Frequency ='M' and WL.EmpName is not null then 'MB' when VW.Frequency ='M' and WL.EmpName is null then 'ME' else VW.Frequency end as Frequency,
case when L.LoanStatus not in ('V','W','G','K') then L.OriginatedAmount else null end as OriginatedAmount, OriginationDate,
case when ((L.RenewalLoanId <> '0') or (A.LPCampaign = 'RENEW'))  then 'RENEWAL' when ApplicationSteps not like '%R%' then 'NEW' else 'REPEAT' end as CustType, 
-- RenewalLoanId, 
A.LPCampaign,
LP.Provider_name 
into #t1
from Application A
left join Loans L on A.Application_ID = L.ApplicationID and A.PortFolioID = L.PortFolioID
left join LeadProvider LP on A.LeadProviderID = LP.LeadProviderID
left join LMS_Logs..VW_ApplicationDump VW on A.APPGUID = VW.APPGUID
left join CustomerReports..EmpWhitelist WL on VW.EmpName=WL.EmpName
where A.ApplicationDate>'2024-01-01' 
-- and A.ApplicationSteps not like '%R%' and A.ApplicationSteps not like '%O%' 
-- and A.LPCampaign in ('UZP006BA1PEX', 'ExpApp_NDD') --For Non-NDD /   for NDD A.LPCampaign in ('UZP006BA1PEX', 'ExpApp_NDD')
and L.LoanStatus not in ('V','W','G','K')
order by A.ApplicationDate asc

-- select * from APplication where LPCampaign in ('UZP006BA1PEX') order by ApplicationDate --, 'ExpApp_NDD')
-- select * from APplication where LPCampaign in ('ExpApp_NDD') order by ApplicationDate --, 'ExpApp_NDD')




-- UPDATE #t1 -- update the renewal loanid from 0 to correct previous loanIDs
-- SET #t1.RenewalLoanId = B.RenewalLoanId
-- from #t1 A inner join (select * from
-- (select A.LoanID, L.LoanID as RenewalLoanId, row_number() over (partition by A.LoanID order by datediff(day, L.OriginationDate, A.OriginationDate) desc) as RN from #t1 A
-- inner join Application A2 on A.CustomerID = A2.CustomerID and A2.ApplicationDate < A.ApplicationDate
-- inner join Loans L on A2.Application_ID = L.ApplicationID and A2.PortFolioID = L.PortFolioID and L.OriginationDate < A.OriginationDate and L.LoanStatus not in ('W','V','G','K')
-- where A.CustType = 'RENEWAL' and A.RenewalLoanId = '0') K where RN=1) B on A.LoanID = B.LoanID



-- scoring result for PlaidUDW_v1 and NegativeDBModelLP_v1
-- drop table if EXISTS #t2
-- select *
-- into #t2 from
-- (select A.*, B1.ExtResponse as B1ExtResponse, B2.ExtResponse as B2ExtResponse,
-- NDScore, IBVScore, NDBand,IBVBand,
-- row_number() over (partition by A.Application_ID, A.PortfolioID order by case when B2.ExtResponse is not null and B1.ExtResponse <> '' then 1 else 0 end desc,B.ApplicationDate desc) as RN
-- from #t1 A 
-- left join Application B on A.CustomerID = B.CustomerID and A.ApplicationDate >= B.ApplicationDate and B.ApplicationSteps like '%S%'
-- left join ScoringPythonResult B1 CROSS APPLY OPENJSON(B1.ExtResponse) WITH (IBVScore int '$.ModelScore', IBVBand int '$.IBVBand')  on B.Application_ID = B1.ApplicationID and B.PortFolioID = B1.PortfolioID and B1.iLabel= 'IBVBand' 
-- left join ScoringPythonResult B2 CROSS APPLY OPENJSON(B2.ExtResponse) WITH (NDScore int '$.ModelScore', NDBand int '$.NDBand') on B.Application_ID = B2.ApplicationID and B.PortFolioID = B2.PortfolioID and B2.iLabel= 'NDBand' ) K where RN=1

-- ALTER TABLE #t2
-- DROP COLUMN RN;

drop table if EXISTS #t3
-- FPDFA original version
select Application_ID, PortFolioID, ApplicationDate, LoanID, CustType, min(FPDFA) as FPDFA, A.Frequency into #t3 from
(select A.*, case when P.PaymentStatus = 'R' then 1 else 0 end as FPDFA, CASE WHEN P.PaymentStatus = 'D' then P.PaymentAmount else 0 end as Paidoff1stInst
from #t1 A 
inner join Payment P on A.LoanID = P.LoanID and P.PaymentMode not in ('V','P','T','H') and PaymentType in ('I','A')
                        and P.InstallmentNumber=1 and P.AttemptNo = 1 and P.PaymentDate <= getdate() and P.PaymentStatus in ('D','R','S','B')
where A.Originated=1) A group by Application_ID, PortFolioID, ApplicationDate, LoanID, CustType, Frequency

select top 10 * from #t3

-- after all installments made, still return? 
-- FPDAA original version 
drop table if EXISTS #t31
select Application_ID, PortFolioID, ApplicationDate, LoanID,CustType, (1-max(FstInstPaidOff)) as FPDAA, Frequency
into #t31 from
(select A.*, case when P.PaymentStatus = 'D' then 1 else 0 end as FstInstPaidOff, P.PaymentStatus
from #t1 A 
inner join Payment P on A.LoanID = P.LoanID and P.PaymentMode not in ('V','P','T','H') and PaymentType in ('I','S','Z','A','X','Q','~','3')
                        and P.InstallmentNumber=1 and P.AttemptNo >= 1 and P.PaymentDate <= getdate() and P.PaymentStatus in ('D','R','S','B')
where A.Originated=1) K group by Application_ID, PortFolioID, ApplicationDate, LoanID, CustType, Frequency

drop table if EXISTS #t3amanda
select ApplicationDate, LoanID, CustType, min(FPDFA) as FPDFA, A.Frequency, A.InstallmentID into #t3amanda from
(select A.*, P.installmentID, case when P.PaymentStatus = 'R' then 1 else 0 end as FPDFA, CASE WHEN P.PaymentStatus = 'D' then P.PaymentAmount else 0 end as Paidoff1stInst
from #t1 A 
inner join Payment P on A.LoanID = P.LoanID and P.PaymentMode in ('A','B','D') and PaymentType not in ('W','9','E','F','K','T','U')
                        and P.InstallmentNumber=1 and P.PaymentDate <= getdate() and Frequency in ('B','W')
where A.Originated=1) A group by InstallmentID, ApplicationDate, LoanID, CustType, Frequency

drop table if exists #t32 
select Application_ID, PortFolioID, ApplicationDate, LoanID,CustType, (1-max(FstInstPaidOff)) as FPDAA, Frequency
into #t32 from
(select A.*, case when P.PaymentStatus = 'D' then 1 else 0 end as FstInstPaidOff, P.PaymentStatus
from #t1 A 
inner join Payment P on A.LoanID = P.LoanID and P.PaymentMode in ('A','B','D') and PaymentType in ('I','S','Z','A','X','Q','~','3')
                        and P.InstallmentNumber=1 and P.AttemptNo >= 1 and P.PaymentDate <= getdate() and P.PaymentStatus in ('D','R','S','B')
where A.Originated=1) K group by Application_ID, PortFolioID, ApplicationDate, LoanID, CustType, Frequency


select top 10 InstallmentID from Payment

select year(ApplicationDate), month(ApplicationDate), count(distinct InstallmentID)
from #t3amanda 
where FPDFA = 1 
group by year(ApplicationDate), month(ApplicationDate)

drop table if EXISTS #t4
select A.Application_ID, A.PortFolioID, A.LoanID, sum(case when P.PaymentStatus = 'D' then P.PaymentAmount else 0 end) as PaidOffPaymentAmount,
sum(case when P.PaymentStatus = 'R' then 1 else 0 end) as PmtReturn, count(*) as PmtCount
into #t4
from #t1 A 
inner join Payment P on A.LoanID = P.LoanID and P.PaymentMode not in ('V','P','T','H') and PaymentType in ('I','S','Z','A','X','Q','~','3') 
                        and P.InstallmentNumber >= 1 and P.PaymentDate <= getdate() and P.PaymentStatus in ('D','R') and P.PaymentAmount>2.95 and (P.PaymentAmount > P.FeeChargePaid)
where A.Originated=1
group by A.Application_ID, A.PortFolioID, A.LoanID


-- drop table if EXISTS #t41
-- select A.Application_ID, A.PortFolioID, A.LoanID, sum(case when P.PaymentStatus = 'D' then P.PaymentAmount else 0 end) as RenewalPaymentAmount
-- into #t41
-- from #t1 A 
-- inner join Loans L on A.LoanID = L.RenewalLoanId
-- inner join Payment P on L.LoanID = P.LoanID and P.PaymentMode ='R' and PaymentType ='R'
--                         and P.InstallmentNumber = 0 and P.PaymentDate <= getdate() and P.PaymentStatus = 'D' and P.PaymentAmount>2.95
-- where A.Originated=1
-- group by A.Application_ID, A.PortFolioID, A.LoanID



-- Final Output: For weekly report
-- assumption: AdCost per web application is $10 (Jeff checked the numbers) + aftercollection 1/3 of defaults will be cleared + Payment Processor cost are the same + agent salary: 4 agents but Juff suggest we take 2 because they're not fully engaged in loonie * 8 hours * 5 days * 17.50 cad per hour
-- select year(A.ApplicationDate) as Year, datepart(week,A.ApplicationDate) as Week, count(*) as Count, sum(A.NewlyScored) as Scored, sum(A.Accepted) as Accepted,
-- sum(A.LeadPurchasePrice) as TotalBidPrice, avg(case when A.LeadPurchasePrice >= 5 and A.LeadPurchasePrice is not null then A.LeadPurchasePrice else null end) as AvgBidPrice,
-- sum(A.Accepted+0.0) / count(*) as AcceptRate, -- can also be count(*): if there're many leads who had been Scored before
-- sum(A.Originated) as Originated,
-- sum(A.Originated+0.0) / (sum(A.Accepted)+0.0000001) as ConvRate,
-- sum(case when A.Originated = 1 then A.OriginatedAmount else 0 end) as TotalFundOut,
-- sum(A.OriginatedAmount+0.0)/sum(A.Originated) as AvgFundOut,
-- sum(case when FPDFA is not null then 1 else 0 end) as NumofLoansDue,
-- sum(FPDFA) as FPDFA,
-- sum(FPDFA+0.0) / sum(case when FPDFA is not null then 1 else 0 end) as FPDFARate,
-- sum(FPDAA) as FPDAA,
-- sum(FPDAA+0.0) / sum(case when FPDAA is not null then 1 else 0 end) as FPDAARate,
-- 1.6 * (1-sum(FPDFA+0.0)*2/3 / sum(case when FPDFA is not null then 1 else 0 end)) as Payin,
-- case when sum(case when A.Originated = 1 then A.OriginatedAmount else 0 end)=0 then null else (sum(A.LeadPurchasePrice) + 10 * sum(case when A.Provider_name='WEB' then 1 else 0 end) + 1.75*count(*) + 1400/4 + 4000/4 + 2*8*5*17.5) / sum(case when A.Originated = 1 then A.OriginatedAmount else 0 end) end as TotalCostPerOrig$,
-- case when sum(case when A.Originated = 1 then A.OriginatedAmount else 0 end)=0 then null else 1.6 * (1-sum(FPDFA+0.0)*2/3 / sum(case when FPDFA is not null then 1 else 0 end)) - 1 - (sum(A.LeadPurchasePrice) + 10*1.5 * sum(case when A.Provider_name='WEB' then 1 else 0 end) + 1.75*count(*) + 1400/4 + 4000/4 + 2*8*5*17.5) / sum(case when A.Originated = 1 then A.OriginatedAmount else 0 end) end as ROI,
-- sum(PaidOffPaymentAmount) as PayinToDate, sum(case when RenewalPaymentAmount is null then 0 else RenewalPaymentAmount end) as RolloverToDate,
-- sum(PaidOffPaymentAmount) / sum(case when A.Originated = 1 then A.OriginatedAmount else 0 end) as PayIn_Realized,
-- (sum(PaidOffPaymentAmount)+ sum(case when RenewalPaymentAmount is null then 0 else RenewalPaymentAmount end)) / sum(case when A.Originated = 1 then A.OriginatedAmount else 0 end) as PayInRenewal_Realized,
-- sum(PmtReturn + 0.0) / sum(PmtCount) as ReturnRate, sum(case when RenewalPaymentAmount is null then 0 else 1 end) as NumRollover
-- from #t2 A  -- avg(OriginatedAmount+0.0) as AvgLoanAmount, sum(FPDFA) as FPDFA, sum(FPDFA+0.0)/count(*)*100 as FPDFARate
-- left join #t3 B on A.Application_ID = B.Application_ID and A.PortFolioID = B.PortFolioID
-- left join #t31 B2 on A.Application_ID = B2.Application_ID and A.PortFolioID = B2.PortFolioID
-- left join #t4 C on A.Application_ID = C.Application_ID and A.PortFolioID = C.PortFolioID
-- left join #t41 D on A.Application_ID = D.Application_ID and A.PortFolioID = D.PortFolioID
-- group by year(A.ApplicationDate),datepart(week, A.ApplicationDate)
-- order by Year, Week asc

-- drop table if EXISTS #t5
-- select A.Application_ID, A.PortFolioID, A.LoanID, A.Frequency, A.LPCampaign, A.OriginatedAmount, 
-- year(A.OriginationDate) as OrigYear, month(A.OriginationDate) as OrigMonth, datepart(Week, A.OriginationDate) as OrigWeek,A.OriginationDate, 
-- FPDFA, FPDAA, PaidOffPaymentAmount as TotalRealizedPayin,
-- P.InstallmentNumber, P.PaymentAmount as PaidOffPaymentAmount, P.TransactionDate, year(P.TransactionDate) as PmtYear, month(P.TransactionDate) as PmtMonth, 
-- datediff(day, A.OriginationDate, P.TransactionDate) as Days_Since_Orig, datediff(day, A.OriginationDate, P.TransactionDate)/7 + 1 as Weeks_Since_Orig, 
-- case when PaymentType = 'I' then 'Installment Pmt' when PaymentType = 'X' then 'Reset Pmt' when PaymentType = '~' then 'Arr Pmt'  when PaymentType = 'A' then 'Partial Pmt' 
-- when PaymentType = 'Q' then 'Delinquent Pmt' when PaymentType = '3' then '3rd Party Pmt' when PaymentType = 'R' then 'Amt rolled over for renewnal' else P.PaymentType end as PaymentType,
-- row_number() over(partition by A.LoanID order by P.PaymentDate asc) as Payment_Number,PaymentStatus, datediff(week, A.OriginationDate, getdate()) as weeks_between_orig_now,
-- A.CustType
-- into #t5
-- from #t1 A
-- inner join Payment P on A.LoanID = P.LoanID and P.PaymentMode not in ('V','P','T','H','G') and PaymentType in ('I','S','Z','A','X','Q','~','3') --,'R') 
--                         and P.InstallmentNumber >= 1 and P.PaymentDate <= getdate() and P.PaymentStatus in ('D','R') and P.PaymentAmount>2.95 --and (P.PaymentAmount > P.FeeChargePaid)
-- left join #t3 B on A.Application_ID = B.Application_ID and A.PortFolioID = B.PortFolioID
-- left join #t31 B2 on A.Application_ID = B2.Application_ID and A.PortFolioID = B2.PortFolioID
-- left join #t4 C on A.Application_ID = C.Application_ID and A.PortFolioID = C.PortFolioID
-- -- left join #t41 D on A.Application_ID = D.Application_ID and A.PortFolioID = D.PortFolioID
-- where A.Originated=1


DROP TABLE IF EXISTS #t6
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
    A.Application_ID,
    A.PortFolioID,
    A.LoanID,
    A.Frequency,
    A.LPCampaign,
    A.OriginatedAmount,
    YEAR(A.OriginationDate)                 AS OrigYear,
    MONTH(A.OriginationDate)                AS OrigMonth,
    DATEPART(WEEK, A.OriginationDate)       AS OrigWeek,
    A.OriginationDate,
    FPDFA,
    FPDAA,
    PaidOffPaymentAmount                    AS TotalRealizedPayin,

    -- Payment-level fields
    P.InstallmentNumber,
    P.PaymentAmount                         AS PaidOffPaymentAmount,

    -- DueDate comes ONLY from the schedule (Installments) table
    I.DueDate,

    P.TransactionDate,
    YEAR(P.TransactionDate)                 AS PmtYear,
    MONTH(P.TransactionDate)                AS PmtMonth,
    DATEDIFF(DAY,  A.OriginationDate, P.TransactionDate)        AS Days_Since_Orig,
    DATEDIFF(DAY,  A.OriginationDate, P.TransactionDate)/7 + 1  AS Weeks_Since_Orig,
    CASE P.PaymentType
        WHEN 'I' THEN 'Installment Pmt'
        WHEN 'X' THEN 'Reset Pmt'
        WHEN '~' THEN 'Arr Pmt'
        WHEN 'A' THEN 'Partial Pmt'
        WHEN 'Q' THEN 'Delinquent Pmt'
        WHEN '3' THEN '3rd Party Pmt'
        WHEN 'R' THEN 'Amt rolled over for renewal'
        ELSE P.PaymentType
    END AS PaymentType,
    ROW_NUMBER() OVER (PARTITION BY A.LoanID ORDER BY P.PaymentDate ASC) AS Payment_Number,
    P.PaymentStatus,
    DATEDIFF(WEEK, A.OriginationDate, GETDATE()) AS weeks_between_orig_now,
    A.CustType
INTO #t6
FROM #t1 AS A
INNER JOIN Payment AS P
    ON A.LoanID = P.LoanID
   AND P.PaymentMode NOT IN ('V','P','T','H','G')
   AND P.PaymentType IN ('I','S','Z','A','X','Q','~','3')    -- add 'R' if you need it
   AND P.InstallmentNumber >= 1
   AND P.PaymentDate <= GETDATE()
   AND P.PaymentStatus IN ('D','R')
   AND P.PaymentAmount > 2.95
-- bring in the schedule's DueDate; rn=1 ensures ≤1 schedule row per payment (no duplication)
LEFT JOIN Inst AS I
    ON I.LoanID = P.LoanID
   AND I.InstallmentNumber = P.InstallmentNumber
   AND I.rn = 1
LEFT JOIN #t3  AS B   ON A.Application_ID = B.Application_ID  AND A.PortFolioID = B.PortFolioID
LEFT JOIN #t31 AS B2  ON A.Application_ID = B2.Application_ID AND A.PortFolioID = B2.PortFolioID
LEFT JOIN #t4  AS C   ON A.Application_ID = C.Application_ID  AND A.PortFolioID = C.PortFolioID
-- LEFT JOIN #t41 AS D ON A.Application_ID = D.Application_ID  AND A.PortFolioID = D.PortFolioID
WHERE A.Originated = 1;

-- yield curve data new version 
select * from #t6 order by LoanID, InstallmentNumber, TransactionDate asc 
-- yield curve data
-- select * from #t5 order by LoanID, InstallmentNumber, TransactionDate asc 


-- fpdfa and fpdaa data
select * from #t3
select * from #t31



-- calculate each group's fpdfa
select year(ApplicationDate) as Year, month(ApplicationDate) as Month, Frequency, avg(FPDFA + 0.0) as FPDFA
from #t3
group by year(ApplicationDate), month(ApplicationDate), Frequency
order by year(ApplicationDate), month(ApplicationDate)

select year(ApplicationDate) as Year, month(ApplicationDate) as Month, avg(FPDFA + 0.0) as FPDFA
from #t3 where Custtype = 'NEW'
group by year(ApplicationDate), month(ApplicationDate) order by year(ApplicationDate), month(ApplicationDate)

select year(ApplicationDate) as Year, month(ApplicationDate) as Month, avg(FPDFA + 0.0) as FPDFA
from #t3 where Custtype = 'NEW'
group by year(ApplicationDate), month(ApplicationDate) order by year(ApplicationDate), month(ApplicationDate)


select year(ApplicationDate) as Year, month(ApplicationDate) as Month, avg(FPDAA + 0.0) as FPDAA
from #t31 where Custtype = 'NEW' and Frequency in ('B','W')
group by year(ApplicationDate), month(ApplicationDate) order by year(ApplicationDate), month(ApplicationDate)

select year(ApplicationDate) as Year, month(ApplicationDate) as Month, Frequency, avg(FPDAA + 0.0) as FPDAA
from #t32 where Custtype = 'NEW' and Frequency in ('B','W')
group by year(ApplicationDate), month(ApplicationDate), Frequency order by year(ApplicationDate), month(ApplicationDate)