/* =====================================================================
   jcx_raw_inference_v1.sql
   Inference extract for persisted payin projection models.

   Contract:
   - Same final temp table expected by the notebook: #t17_combined.
   - Recent application cohort only, defaulting to the last 120 days.
   - Normal installments are schedule-complete for non-terminal recent loans.
   - Loans with observed terminal payoff/default are still trimmed through the
     terminal installment.

   This is intentionally separate from jcx_raw_harvey_v14.sql, which remains
   the compact training / teammate extract.
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
    Inst.DueAmount                              AS InstallmentDueAmount,
    CASE WHEN L.LoanStatus NOT IN ('V','W','G','K') THEN L.OriginatedAmount ELSE NULL END AS OriginatedAmount,
    CAST(OriginationDate AS DATE)               AS OriginationDate,
    CASE WHEN (ApplicationSteps NOT LIKE '%R%' AND A.ApplicationSteps NOT LIKE '%O%') THEN 'NEW' ELSE 'RETURN' END AS CustType,
    A.LPCampaign,
    LP.Provider_name
INTO #t1
FROM LMSMaster..Application AS A
LEFT JOIN Loans L ON A.Application_ID = L.ApplicationID AND A.PortFolioID = L.PortFolioID
LEFT JOIN LeadProvider LP ON A.LeadProviderID = LP.LeadProviderID
LEFT JOIN LMS_Logs..VW_ApplicationDump VW ON A.APPGUID = VW.APPGUID
LEFT JOIN Installments AS Inst ON Inst.LoanID = L.LoanID AND Inst.PortFolioId = L.PortFolioID
WHERE A.ApplicationDate >= @InferenceStartDate
  AND A.ApplicationDate <  @InferenceEndDate
  AND L.LoanStatus NOT IN ('V','W','G','K')

/* =====================================================================
   SECTION 2: Loan-Level Total Realized Payments
   ===================================================================== */
DROP TABLE IF EXISTS #t4
SELECT
    A.Application_ID, A.PortFolioID, A.LoanID,
    SUM(CASE WHEN P.PaymentStatus = 'D' THEN P.PaymentAmount ELSE 0 END) AS TotalRealizedPayment
INTO #t4
FROM (SELECT DISTINCT Application_ID, PortFolioID, LoanID FROM #t1) A
INNER JOIN Payment P
    ON A.LoanID = P.LoanID
    AND P.PaymentMode IN ('A','D','K','B')
    AND P.PaymentType IN ('Z','A','I','S','Q','X','~','3')
    AND P.InstallmentNumber >= 1
    AND P.PaymentStatus = 'D'
GROUP BY A.Application_ID, A.PortFolioID, A.LoanID

/* =====================================================================
   SECTION 3a: Normal Installment Payments (iPaymentMode = 144)
   ===================================================================== */
DROP TABLE IF EXISTS #t5a
SELECT
    A.Application_ID, A.PortFolioID, A.LoanID,
    Inst.InstallmentNumber,
    Inst.InstallmentID,
    CAST(Inst.DueDate AS DATE)  AS InstallmentDueDate,
    MAX(CAST(P.PaymentDate AS DATE)) AS PaymentDate,
    SUM(CASE WHEN P.PaymentStatus = 'D' AND P.InstallmentNumber >= 1 THEN P.PaymentAmount ELSE 0 END) AS InstallRealizedPayment,
    MAX(CASE WHEN P.PaymentStatus = 'D' AND PaymentAmount > 0 AND P.PaymentType = '3' THEN 1 ELSE 0 END) AS ThirdPartyCollected,
    MAX(CASE WHEN P.PaymentStatus = 'D' AND PaymentAmount > 0 AND P.PaymentType = 'A' THEN 1 ELSE 0 END) AS PartialCollected,
    MAX(CASE WHEN P.PaymentStatus = 'D' AND PaymentAmount > 0 AND P.PaymentType = 'I' THEN 1 ELSE 0 END) AS InstallCollected,
    MAX(CASE WHEN P.PaymentStatus = 'D' AND PaymentAmount > 0 AND P.PaymentType = 'Z' THEN 1 ELSE 0 END) AS EarlyCollected
INTO #t5a
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
    AND P.PaymentType IN ('Z','A','I','S','Q','X')
    AND P.PaymentStatus IN ('D', 'F')
GROUP BY A.Application_ID, A.PortFolioID, A.LoanID,
         Inst.InstallmentNumber, Inst.InstallmentID, CAST(Inst.DueDate AS DATE)

/* =====================================================================
   SECTION 3b: Arrangement Payments (iPaymentMode = 679)
   ===================================================================== */
DROP TABLE IF EXISTS #t5b
SELECT
    A.Application_ID, A.PortFolioID, A.LoanID,
    Inst.InstallmentNumber,
    Inst.InstallmentID,
    Inst.[Status]                                                           AS installStatus,
    CAST(Inst.DueDate AS DATE)                                              AS ArrangementDueDate,
    SUM(CASE WHEN P.PaymentStatus = 'D' THEN P.PaymentAmount ELSE 0 END)   AS ArrangementRealizedPayment,
    MAX(CAST(P.PaymentDate AS DATE)) AS PaymentDate
INTO #t5b
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
    AND P.PaymentStatus = 'D'
WHERE Inst.DueDate < @InferenceEndDate
GROUP BY A.Application_ID, A.PortFolioID, A.LoanID,
         Inst.InstallmentNumber, Inst.InstallmentID, Inst.[Status], CAST(Inst.DueDate AS DATE)

/* =====================================================================
   SECTION 3c: 3rd Party Payments (iPaymentMode = 685)
   ===================================================================== */
DROP TABLE IF EXISTS #t5c
SELECT
    A.Application_ID, A.PortFolioID, A.LoanID,
    Inst.InstallmentNumber,
    Inst.InstallmentID,
    Inst.[Status]                                                           AS installStatus,
    CAST(Inst.DueDate AS DATE)                                              AS ThirdPartyDueDate,
    SUM(CASE WHEN P.PaymentStatus = 'D' THEN P.PaymentAmount ELSE 0 END)   AS ThirdPartyRealizedPayment,
    MAX(CAST(P.PaymentDate AS DATE)) AS PaymentDate
INTO #t5c
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
    AND P.PaymentStatus = 'D'
WHERE Inst.DueDate < @InferenceEndDate
GROUP BY A.Application_ID, A.PortFolioID, A.LoanID,
         Inst.InstallmentNumber, Inst.InstallmentID, Inst.[Status], CAST(Inst.DueDate AS DATE)

/* =====================================================================
   SECTION 3d: Normal Installment DENY NEW Helper
   ===================================================================== */
DROP TABLE IF EXISTS #t_attempt
SELECT DISTINCT A.LoanID,
    MAX(CASE WHEN P.PaymentStatus NOT IN ('P','F')
        AND P.InstallmentNumber >= 1
        AND P.PaymentType IN ('I','Z','S','Q')
        THEN 1 ELSE 0 END) AS hasPaymentAttempt
INTO #t_attempt
FROM (SELECT DISTINCT Application_ID, PortFolioID, LoanID, iPaymentMode FROM #t1) A
INNER JOIN Payment P ON A.LoanID = P.LoanID
    AND P.PaymentMode IN ('A','D','K','B')
    AND P.InstallmentNumber >= 1
    AND A.iPaymentMode = 144
GROUP BY A.LoanID

/* =====================================================================
   SECTION 4: Normal Installment Summary with Flags (#t17a)
   ===================================================================== */
DROP TABLE IF EXISTS #t17a
SELECT
    A.Application_ID,
    YEAR(A.ApplicationDate)                 AS AppYear,
    MONTH(A.ApplicationDate)                AS AppMonth,
    DATEPART(WEEK, A.ApplicationDate)       AS AppWeek,
    A.PortFolioID,
    A.LoanID,
    t4.TotalRealizedPayment,
    A.iPaymentMode,
    A.LoanStatus,
    A.InstallmentNumber,
    A.InstallmentDueDate,
    A.InstallmentDueAmount,
    instA.PaymentDate,
    COALESCE(instA.InstallRealizedPayment, 0)   AS InstallRealizedPayment,
    A.installStatus,

    CASE
        WHEN A.installStatus = 684 AND A.LoanStatus IN ('N') THEN 1
        ELSE 0
    END AS isRecentLoan,

    CASE
        WHEN A.installStatus IN (111, 779)
        AND (
            (
                MIN(A.installStatus) OVER (PARTITION BY A.LoanID ORDER BY A.InstallmentNumber
                    ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) = 115
                AND MAX(A.installStatus) OVER (PARTITION BY A.LoanID ORDER BY A.InstallmentNumber
                    ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) = 115
            )
            OR
            MIN(A.installStatus) OVER (PARTITION BY A.LoanID ORDER BY A.InstallmentNumber
                ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) IS NULL
        )
        THEN 1
        ELSE 0
    END AS LoanPaidOffThisInstall,

    CASE
        WHEN A.installStatus = 825
             AND COALESCE(MAX(CASE WHEN A.installStatus IN (111, 779) THEN 1 ELSE 0 END)
                 OVER (PARTITION BY A.LoanID ORDER BY A.InstallmentNumber
                       ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING), 0) = 1 THEN 0
        WHEN A.installStatus = 825 THEN 1
        WHEN MAX(CASE WHEN A.installStatus IN (111, 779) THEN 1 ELSE 0 END)
             OVER (PARTITION BY A.LoanID ORDER BY A.InstallmentNumber
                   ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) = 1 THEN 0
        WHEN A.installStatus = 684
             AND MIN(A.installStatus) OVER (PARTITION BY A.LoanID) = 684
             AND MAX(A.installStatus) OVER (PARTITION BY A.LoanID) = 684
             AND A.LoanStatus IN ('R')
             AND A.InstallmentNumber = 1 THEN 1
        WHEN A.installStatus = 115
             AND MIN(A.installStatus) OVER (PARTITION BY A.LoanID) = 115
             AND MAX(A.installStatus) OVER (PARTITION BY A.LoanID) = 115
             AND A.InstallmentNumber = 1 THEN 1
        WHEN A.LoanStatus NOT IN ('D', 'P')
             AND A.installStatus NOT IN (111, 115, 779, 684) THEN 1
        WHEN A.installStatus = 684
             AND LAG(A.installStatus) OVER (PARTITION BY A.LoanID ORDER BY A.InstallmentNumber) IN (111, 779)
             AND A.LoanStatus NOT IN ('D', 'P', 'N') THEN 1
        WHEN A.installStatus IN (786, 825)
             AND A.LoanStatus = 'D'
             AND COALESCE(InstallRealizedPayment, 0) = 0
             AND LAG(COALESCE(InstallRealizedPayment, 0)) OVER (PARTITION BY A.LoanID ORDER BY A.InstallmentNumber) > 0 THEN 1
        WHEN A.installStatus = 111 THEN 0
        WHEN A.installStatus IN (115, 779) THEN 0
        WHEN A.installStatus = 684 THEN 0
        ELSE 0
    END AS isLoanDefault,

    CASE
        WHEN A.installStatus = 684
            AND MIN(A.installStatus) OVER (PARTITION BY A.LoanID) = 684
            AND MAX(A.installStatus) OVER (PARTITION BY A.LoanID) = 684
            AND A.LoanStatus IN ('R')
            AND A.InstallmentNumber = 1 THEN 1
        WHEN A.installStatus NOT IN (111, 779, 684) THEN 1
        WHEN A.installStatus = 115
             AND LAG(A.installStatus) OVER (PARTITION BY A.LoanID ORDER BY A.InstallmentNumber) <> 111 THEN 1
        WHEN A.installStatus = 684
             AND LAG(A.installStatus) OVER (PARTITION BY A.LoanID ORDER BY A.InstallmentNumber) IN (111, 779)
             AND A.LoanStatus NOT IN ('D', 'P', 'N') THEN 1
        ELSE 0
    END AS isInstallDefault,

    CASE
        WHEN A.installStatus = 115
            AND MIN(A.installStatus) OVER (PARTITION BY A.LoanID) = 115
            AND MAX(A.installStatus) OVER (PARTITION BY A.LoanID) = 115
            AND A.InstallmentNumber = 1 THEN 1
        ELSE 0
    END AS isAllVoided,

    CASE
        WHEN A.installStatus = 684
            AND MIN(A.installStatus) OVER (PARTITION BY A.LoanID) = 684
            AND MAX(A.installStatus) OVER (PARTITION BY A.LoanID) = 684
            AND A.LoanStatus IN ('R')
            AND COALESCE(att.hasPaymentAttempt, 0) = 0
            AND A.InstallmentNumber = 1 THEN 1
        ELSE 0
    END AS isDenyNew,

    instA.ThirdPartyCollected,
    instA.PartialCollected,
    instA.InstallCollected,
    instA.EarlyCollected

INTO #t17a
FROM #t1 AS A
LEFT JOIN #t4 AS t4
    ON t4.LoanID = A.LoanID
    AND t4.Application_ID = A.Application_ID
    AND t4.PortFolioID = A.PortFolioID
LEFT JOIN #t5a AS instA
    ON A.LoanID = instA.LoanID
    AND A.InstallmentID = instA.InstallmentID
    AND A.InstallmentNumber = instA.InstallmentNumber
LEFT JOIN #t_attempt att ON A.LoanID = att.LoanID
WHERE A.iPaymentMode = 144

/* =====================================================================
   SECTION 5: Trim Normal Installments (#t17a_final)

   Inference-specific behavior:
   - Terminal loans: keep rows through the observed terminal event.
   - Non-terminal recent loans: keep the full scheduled normal installment
     path, including not-yet-due / unpaid future rows.
   ===================================================================== */
DROP TABLE IF EXISTS #t17a_final
SELECT *,
    COUNT(*) OVER (PARTITION BY LoanID) AS TotalInstallsNumber
INTO #t17a_final
FROM #t17a t17a
WHERE (
    t17a.InstallmentNumber <= (
        SELECT MIN(sub.InstallmentNumber)
        FROM #t17a sub
        WHERE sub.LoanID = t17a.LoanID
          AND (sub.LoanPaidOffThisInstall = 1 OR sub.isLoanDefault = 1)
    )
)
OR (
    NOT EXISTS (
        SELECT 1 FROM #t17a sub
        WHERE sub.LoanID = t17a.LoanID
          AND (sub.LoanPaidOffThisInstall = 1 OR sub.isLoanDefault = 1)
    )
)

/* =====================================================================
   SECTION 5b: Underwriting Risk Bands (Late Join Source)
   ===================================================================== */
DROP TABLE IF EXISTS #sa_bands
SELECT
    Application_ID,
    PortFolioID,
    MAX(DM_Band_Name) AS DM_Band_Name,
    MAX(CM_Band_Name) AS CM_Band_Name
INTO #sa_bands
FROM QlikDB..ScoredApplications
WHERE Application_ID IS NOT NULL
GROUP BY Application_ID, PortFolioID

/* =====================================================================
   SECTION 6: Combined Final Table (Normal + Arrangement + 3rd Party)
   ===================================================================== */
DROP TABLE IF EXISTS #t17_stacked
SELECT
    Application_ID, PortFolioID, LoanID,
    InstallmentNumber, InstallRealizedPayment, installStatus, iPaymentMode,
    TotalInstallsNumber, InstallmentDueDate, InstallmentDueAmount, PaymentDate,
    isRecentLoan, LoanPaidOffThisInstall, isLoanDefault, isInstallDefault,
    ThirdPartyCollected, PartialCollected, InstallCollected, EarlyCollected,
    isDenyNew, isAllVoided,
    0 AS isArrangementInstall, 0 AS is3rdPartyInstall
INTO #t17_stacked
FROM #t17a_final

UNION ALL

SELECT
    a.Application_ID, a.PortFolioID, a.LoanID,
    b.InstallmentNumber, b.ArrangementRealizedPayment, b.installStatus, 679 AS iPaymentMode,
    a.TotalInstallsNumber, b.ArrangementDueDate AS InstallmentDueDate, NULL AS InstallmentDueAmount, PaymentDate,
    0, 0, 0, 0,
    0, 0, 0, 0,
    0, 0,
    1, 0
FROM (SELECT DISTINCT Application_ID, PortFolioID, LoanID, TotalInstallsNumber
      FROM #t17a_final) a
INNER JOIN #t5b b ON a.LoanID = b.LoanID

UNION ALL

SELECT
    a.Application_ID, a.PortFolioID, a.LoanID,
    c.InstallmentNumber, c.ThirdPartyRealizedPayment, c.installStatus, 685 AS iPaymentMode,
    a.TotalInstallsNumber, c.ThirdPartyDueDate AS InstallmentDueDate, NULL AS InstallmentDueAmount, PaymentDate,
    0, 0, 0, 0,
    0, 0, 0, 0,
    0, 0,
    0, 1
FROM (SELECT DISTINCT Application_ID, PortFolioID, LoanID, TotalInstallsNumber
      FROM #t17a_final) a
INNER JOIN #t5c c ON a.LoanID = c.LoanID

DROP TABLE IF EXISTS #t17_combined
SELECT
    s.*,
    t4.TotalRealizedPayment,
    t1.AppYear, t1.AppMonth, t1.AppWeek,
    t1.LoanStatus,  t1.CustType, t1.Frequency,
    t1.OriginatedAmount, t1.OriginationDate,
    sb.DM_Band_Name,
    sb.CM_Band_Name
INTO #t17_combined
FROM #t17_stacked s
LEFT JOIN #t4 t4 ON s.LoanID = t4.LoanID AND s.Application_ID = t4.Application_ID
LEFT JOIN (SELECT DISTINCT Application_ID, PortFolioID, LoanID,
                  AppYear, AppMonth, AppWeek, LoanStatus, CustType,
                  Frequency, OriginatedAmount, OriginationDate
           FROM #t1) t1
    ON s.LoanID = t1.LoanID AND s.Application_ID = t1.Application_ID
LEFT JOIN #sa_bands sb
    ON s.Application_ID = sb.Application_ID
   AND s.PortFolioID    = sb.PortFolioID

DROP TABLE IF EXISTS #t17_mfg
SELECT *,
    ROW_NUMBER() OVER (PARTITION BY LoanID ORDER BY iPaymentMode, InstallmentNumber) AS InstallmentNumberMFG
INTO #t17_mfg
FROM #t17_combined
