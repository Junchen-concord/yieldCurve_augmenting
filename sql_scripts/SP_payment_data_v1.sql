/* =====================================================================
   SP_payment_data_v1.sql
   Payment-attempt-grain extract — companion to jcx_raw_harvey_v15.sql.

   v15 ships installment-rollup grain (#t5a / #t5b / #t5c).
   This file ships the same three-stream split, but at attempt grain:
   one row per (LoanID x InstallmentID x AttemptNo). Stage B feature
   engineering consumes #p5a; Stage C upgrade (per-loan recovery model,
   Option 2) consumes #p5b + #p5c.

   Scope decisions (signed off 2026-04-29):
   - PaymentStatus filter: 'D' (Paid Off / success) and 'R' (Returned /
     fail) only. Other statuses (V, I, S, P, ...) are deliberately
     excluded for the MVP and may be widened later. Pending (P / 684)
     falls out naturally because we keep only D and R.
   - PaymentMode filter: 'A','D','K','B' (ACH, Debit, Check, RCC).
     NOTE: mirrors v15. Could broaden later (see status_reference.csv
     CategoryID = 24 for the full list).
   - No PaymentType filter (we want every attempt, not just installment
     payments).
   - No GROUP BY (the whole point is the per-attempt sequence).
   - No DueDate <= GETDATE() filter — early-paid installments still
     carry attempt rows we want.
   ===================================================================== */
USE LMSMaster;

/* =====================================================================
   SECTION 1: Base Application Table (Loan x Installment x Plan)
   Copied from jcx_raw_harvey_v15.sql §1 so this file runs standalone.
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
WHERE A.ApplicationDate >= '2023-01-01'
  AND L.LoanStatus NOT IN ('V','W','G','K')


/* =====================================================================
   SECTION 2a: Normal Installment Payment Attempts (iPaymentMode = 144)
   Stage B consumer.
   installStatus is omitted here (always 144 on the normal stream).
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
    AND P.PaymentMode IN ('A','D','K','B')   -- NOTE: mirrors v15. Broaden later if needed.
    AND P.PaymentStatus IN ('D','R')          -- MVP: success + returned only.


/* =====================================================================
   SECTION 2b: Arrangement Payment Attempts (iPaymentMode = 679)
   Stage C consumer (per-loan recovery model upgrade path).
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
    AND P.InstallmentNumber = A.InstallmentNumber
    AND P.InstallmentID = Inst.InstallmentID
    AND P.PaymentMode IN ('A','D','K','B')   -- NOTE: mirrors v15. Broaden later if needed.
    AND P.PaymentStatus IN ('D','R')


/* =====================================================================
   SECTION 2c: 3rd Party Payment Attempts (iPaymentMode = 685)
   Stage C consumer (per-loan recovery model upgrade path).
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
    AND P.InstallmentNumber = A.InstallmentNumber
    AND P.InstallmentID = Inst.InstallmentID
    AND P.PaymentMode IN ('A','D','K','B')   -- NOTE: mirrors v15. Broaden later if needed.
    AND P.PaymentStatus IN ('D','R')


/* =====================================================================
   SECTION 3: Final result sets (one per stream)
   The notebook reads these in order:
     #p5a -> payment_normal_df    (Stage B)
     #p5b -> payment_arr_df       (Stage C)
     #p5c -> payment_3p_df        (Stage C)
   ===================================================================== */
SELECT * FROM #p5a;
SELECT * FROM #p5b;
SELECT * FROM #p5c;
