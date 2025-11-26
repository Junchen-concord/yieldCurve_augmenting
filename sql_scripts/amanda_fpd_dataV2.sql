DROP TABLE IF EXISTS #LoanDefault
SELECT L.LoanID, L.ApplicationID AS Application_ID, A.ApplicationDate, A.ApplicationSteps,
CASE
        WHEN VW.Frequency IN ('B','S') THEN 'B'
        WHEN VW.Frequency = 'M' AND WL.EmpName IS NOT NULL THEN 'MB'
        WHEN VW.Frequency = 'M' AND WL.EmpName IS NULL  THEN 'ME'
        ELSE VW.Frequency
END AS Frequency,
L.OriginationDate,
L.PortFolioID, L.LoanStatus,
P.InstallmentNumber, P.PaymentStatus, P.PaymentType, P.PaymentMode, P.AttemptNo, P.TransactionDate, P.PaymentID,
I.InstallmentID, I.iPaymentMode, I.DueDate, I.Status, -- used to exclude pendings (code 684)
(CASE WHEN I.Status=684 THEN 1 ELSE 0 END) AS Pending
INTO #LoanDefault
FROM LMSMaster..Loans L
LEFT JOIN LMSMaster..Payment P ON P.LoanID = L.LoanID
LEFT JOIN LMSMaster..Installments I ON I.InstallmentID = P.InstallmentID
LEFT JOIN LMSMaster..Application A ON A.PortfolioID=L.PortfolioID AND A.Application_ID = L.ApplicationID
LEFT JOIN LMS_Logs..VW_ApplicationDump VW ON A.APPGUID = VW.APPGUID
LEFT JOIN CustomerReports..EmpWhitelist WL ON VW.EmpName = WL.EmpName
WHERE A.ApplicationDate >= '2024-08-01' AND A.ApplicationDate < '2025-11-01' 
AND L.OriginationDate IS NOT NULL
AND I.InstallmentNumber IS NOT NULL
AND I.InstallmentNumber >= 1

DROP TABLE IF EXISTS #LoanDefault_Flag
SELECT 
    L.*,
    -- FPDFA flag
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
           InstallmentNumber, OriginationDate, Frequency, PaymentStatus, PaymentType, PaymentMode, AttemptNo, TransactionDate,
           PaymentID, InstallmentID, iPaymentMode, DueDate, Status, Pending, is_FPDFA, is_loan_first_install,
           ROW_NUMBER() OVER (PARTITION BY Application_ID, PortfolioID ORDER BY is_FPDFA DESC) AS rn
    FROM #LoanDefault_Flag
)
SELECT LoanID, Application_ID, ApplicationDate, ApplicationSteps, PortfolioID, LoanStatus,
       InstallmentNumber, OriginationDate, Frequency, PaymentStatus, PaymentType, PaymentMode, AttemptNo, TransactionDate,
       PaymentID, InstallmentID, iPaymentMode, DueDate, Status, Pending, is_FPDFA, is_loan_first_install
INTO #LoanDefault_Dedup
FROM dedup
WHERE rn = 1;

SELECT 
    FORMAT(OriginationDate, 'yyyy-MM') AS OrigMonth,
    InstallmentNumber,  
    Frequency,
    COUNT(DISTINCT CASE WHEN is_FPDFA = 1 THEN LoanID END) AS FPDFA_count_all,
    COUNT(DISTINCT CASE WHEN is_loan_first_install = 1 THEN LoanID END) AS first_install_loan_count_all,
    CAST(COUNT(DISTINCT CASE WHEN is_FPDFA = 1 THEN LoanID END) AS decimal(18,4))
    / NULLIF(COUNT(DISTINCT CASE WHEN is_loan_first_install = 1 THEN LoanID END), 0)  AS FPDFA_rate
FROM #LoanDefault_Dedup
GROUP BY 
    FORMAT(OriginationDate, 'yyyy-MM'),
    InstallmentNumber,
    Frequency
ORDER BY OrigMonth, InstallmentNumber
