/* =====================================================================
   SP_payment_data_inference_v1.sql
   Recent-cohort payment-attempt-grain extract for inference.

   Contract:
   - Returns the same three result sets as SP_payment_data_v1.sql:
       #p5a -> normal payment attempts
       #p5b -> arrangement payment attempts
       #p5c -> third-party payment attempts
   - Scopes loans to the same recent ApplicationDate window as
     jcx_raw_inference_v1.sql.
   - Does not manufacture empty attempt rows. Loans/installments without
     attempts are handled by Python feature builders as zero-history cases.
   ===================================================================== */
USE LMSMaster;

DECLARE @InferenceAsOfDate DATE = CAST(GETDATE() AS DATE);
DECLARE @InferenceLookbackDays INT = 120;
DECLARE @InferenceStartDate DATE = DATEADD(DAY, -@InferenceLookbackDays, @InferenceAsOfDate);
DECLARE @InferenceEndDate DATE = DATEADD(DAY, 1, @InferenceAsOfDate);

/* =====================================================================
   SECTION 1: Base Application Table (Loan x Installment x Plan)
   ===================================================================== */
DROP TABLE IF EXISTS #t1
SELECT
    A.Application_ID, A.PortfolioID, A.CustomerID, A.ApplicationDate,
    YEAR(A.ApplicationDate)                     AS AppYear,
    MONTH(A.ApplicationDate)                    AS AppMonth,
    DATEPART(WEEK, A.ApplicationDate)           AS AppWeek,
    DATEDIFF(YEAR, VW.DOB, A.ApplicationDate)   AS Age,
    VW.Frequency,
    L.LoanID,
    L.LoanStatus,
    Inst.InstallmentNumber,
    Inst.[Status]                               AS installStatus,
    Inst.InstallmentID,
    Inst.iPaymentMode,
    CAST(Inst.DueDate AS DATE)                  AS InstallmentDueDate,
    CASE WHEN L.LoanStatus NOT IN ('V','W','G','K') THEN L.OriginatedAmount ELSE NULL END AS OriginatedAmount,
    CAST(OriginationDate AS DATE)               AS OriginationDate,
    CASE WHEN (ApplicationSteps NOT LIKE '%R%' AND A.ApplicationSteps NOT LIKE '%O%') THEN 'NEW' ELSE 'RETURN' END AS CustType,
    A.LPCampaign,
    LP.Provider_name
INTO #t1
FROM LMSMaster..Application AS A
LEFT JOIN Loans L           ON A.Application_ID = L.ApplicationID AND A.PortFolioID = L.PortFolioID
LEFT JOIN LeadProvider LP   ON A.LeadProviderID = LP.LeadProviderID
LEFT JOIN LMS_Logs..VW_ApplicationDump VW ON A.APPGUID = VW.APPGUID
LEFT JOIN Installments Inst ON Inst.LoanID = L.LoanID AND Inst.PortFolioId = L.PortFolioID
WHERE A.ApplicationDate >= @InferenceStartDate
  AND A.ApplicationDate <  @InferenceEndDate
  AND L.LoanStatus NOT IN ('V','W','G','K')

/* =====================================================================
   SECTION 2a: Normal Installment Payment Attempts (iPaymentMode = 144)
   ===================================================================== */
DROP TABLE IF EXISTS #p5a
SELECT
    A.Application_ID, A.PortFolioID, A.LoanID,
    Inst.InstallmentNumber,
    Inst.InstallmentID,
    Inst.iPaymentMode,
    CAST(Inst.DueDate AS DATE)                          AS InstallmentDueDate,
    P.PaymentID,
    P.AttemptNo,
    CAST(P.PaymentDate AS DATE)                         AS PaymentDate,
    CAST(P.TransactionDate AS DATE)                     AS TransactionDate,
    P.PaymentAmount,
    P.PaymentStatus,
    P.PaymentType,
    P.PaymentMode,
    CASE WHEN P.PaymentStatus = 'D' THEN 1 ELSE 0 END   AS IsSuccess,
    CASE WHEN P.PaymentStatus = 'R' THEN 1 ELSE 0 END   AS IsFail
INTO #p5a
FROM #t1 A
INNER JOIN Installments Inst
    ON A.LoanID = Inst.LoanID
    AND A.PortFolioID = Inst.PortFolioId
    AND Inst.iPaymentMode = 144
    AND Inst.InstallmentID = A.InstallmentID
INNER JOIN Payment P
    ON P.LoanID = Inst.LoanID
    AND P.InstallmentNumber = A.InstallmentNumber
    AND P.InstallmentID = Inst.InstallmentID
    AND P.PaymentMode IN ('A','D','K','B')
    AND P.PaymentStatus IN ('D','R')

/* =====================================================================
   SECTION 2b: Arrangement Payment Attempts (iPaymentMode = 679)
   ===================================================================== */
DROP TABLE IF EXISTS #p5b
SELECT
    A.Application_ID, A.PortFolioID, A.LoanID,
    Inst.InstallmentNumber,
    Inst.InstallmentID,
    Inst.iPaymentMode,
    Inst.[Status]                                       AS installStatus,
    CAST(Inst.DueDate AS DATE)                          AS InstallmentDueDate,
    P.PaymentID,
    P.AttemptNo,
    CAST(P.PaymentDate AS DATE)                         AS PaymentDate,
    CAST(P.TransactionDate AS DATE)                     AS TransactionDate,
    P.PaymentAmount,
    P.PaymentStatus,
    P.PaymentType,
    P.PaymentMode,
    CASE WHEN P.PaymentStatus = 'D' THEN 1 ELSE 0 END   AS IsSuccess,
    CASE WHEN P.PaymentStatus = 'R' THEN 1 ELSE 0 END   AS IsFail
INTO #p5b
FROM #t1 A
INNER JOIN Installments Inst
    ON A.LoanID = Inst.LoanID
    AND A.PortFolioID = Inst.PortFolioId
    AND Inst.iPaymentMode = 679
    AND Inst.InstallmentID = A.InstallmentID
INNER JOIN Payment P
    ON P.LoanID = Inst.LoanID
    AND P.InstallmentNumber = Inst.InstallmentNumber
    AND P.InstallmentID = Inst.InstallmentID
    AND P.PaymentMode IN ('A','D','K','B')
    AND P.PaymentStatus IN ('D','R')

/* =====================================================================
   SECTION 2c: 3rd Party Payment Attempts (iPaymentMode = 685)
   ===================================================================== */
DROP TABLE IF EXISTS #p5c
SELECT
    A.Application_ID, A.PortFolioID, A.LoanID,
    Inst.InstallmentNumber,
    Inst.InstallmentID,
    Inst.iPaymentMode,
    Inst.[Status]                                       AS installStatus,
    CAST(Inst.DueDate AS DATE)                          AS InstallmentDueDate,
    P.PaymentID,
    P.AttemptNo,
    CAST(P.PaymentDate AS DATE)                         AS PaymentDate,
    CAST(P.TransactionDate AS DATE)                     AS TransactionDate,
    P.PaymentAmount,
    P.PaymentStatus,
    P.PaymentType,
    P.PaymentMode,
    CASE WHEN P.PaymentStatus = 'D' THEN 1 ELSE 0 END   AS IsSuccess,
    CASE WHEN P.PaymentStatus = 'R' THEN 1 ELSE 0 END   AS IsFail
INTO #p5c
FROM #t1 A
INNER JOIN Installments Inst
    ON A.LoanID = Inst.LoanID
    AND A.PortFolioID = Inst.PortFolioId
    AND Inst.iPaymentMode = 685
    AND Inst.InstallmentID = A.InstallmentID
INNER JOIN Payment P
    ON P.LoanID = Inst.LoanID
    AND P.InstallmentNumber = Inst.InstallmentNumber
    AND P.InstallmentID = Inst.InstallmentID
    AND P.PaymentMode IN ('A','D','K','B')
    AND P.PaymentStatus IN ('D','R')

/* =====================================================================
   SECTION 3: Final result sets (one per stream)
   ===================================================================== */
SELECT * FROM #p5a;
SELECT * FROM #p5b;
SELECT * FROM #p5c;
