/* =====================================================================
   jcx_raw_harvey_v15_clean.sql
   Clean production version — no debug queries
   ===================================================================== */
USE LMSMaster;

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
LEFT JOIN Loans L ON A.Application_ID = L.ApplicationID AND A.PortFolioID = L.PortFolioID
LEFT JOIN LeadProvider LP ON A.LeadProviderID = LP.LeadProviderID
LEFT JOIN LMS_Logs..VW_ApplicationDump VW ON A.APPGUID = VW.APPGUID
LEFT JOIN Installments AS Inst ON Inst.LoanID = L.LoanID AND Inst.PortFolioId = L.PortFolioID
WHERE A.ApplicationDate >= '2023-01-01'
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
    CAST(Inst.DueDate AS DATE)  AS InstallmentDueDate, -- append dueDate to the data processing
    MAX(CAST(P.TransactionDate AS DATE)) AS TransactionDate, 
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
    AND P.PaymentType IN ('Z','A','I','S','Q','X') --  (took out F Type)include installment 0 for DENY NEW Case
    AND P.PaymentStatus IN ('D', 'F')
--WHERE Inst.DueDate < GETDATE()
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
    MAX(CAST(P.TransactionDate AS DATE)) AS TransactionDate
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
    --AND P.PaymentType = '~'
    AND P.PaymentStatus = 'D'
WHERE Inst.DueDate < GETDATE()
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
    MAX(CAST(P.TransactionDate AS DATE)) AS TransactionDate
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
    --AND P.PaymentType = '3'
    AND P.PaymentStatus = 'D'
WHERE Inst.DueDate < GETDATE()
GROUP BY A.Application_ID, A.PortFolioID, A.LoanID,
         Inst.InstallmentNumber, Inst.InstallmentID, Inst.[Status], CAST(Inst.DueDate AS DATE)

/* =====================================================================
   SECTION 3a: Normal Installment DENY NEW Helper
   ===================================================================== */
DROP TABLE IF EXISTS #t_attempt
SELECT DISTINCT A.LoanID,
    MAX(CASE WHEN P.PaymentStatus NOT IN ('P','F') 
        AND P.InstallmentNumber >= 1 
        AND P.PaymentType IN ('I','Z','S','Q','A')
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
    A.InstallmentDueDate,  -- append installment Due date
    COALESCE(instA.InstallRealizedPayment, 0)   AS InstallRealizedPayment,
    A.installStatus,

    /* *************** isRecentLoan *************** */
    CASE
        WHEN A.installStatus = 684 AND A.LoanStatus IN ('N') THEN 1
        ELSE 0
    END AS isRecentLoan,

    /* *************** LoanPaidOffThisInstall *************** */
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

    /* *************** isLoanDefault *************** */
CASE
    -- 825 with recovery ahead — not terminal
WHEN A.installStatus = 825
     AND COALESCE(MAX(CASE WHEN A.installStatus IN (111, 779) THEN 1 ELSE 0 END)
         OVER (PARTITION BY A.LoanID ORDER BY A.InstallmentNumber
               ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING), 0) = 1    THEN 0

-- 825 with no recovery ahead — terminal
WHEN A.installStatus = 825                                                  THEN 1

    -- Any bad install (including 684 mid-loan) but good install follows — not terminal
    WHEN MAX(CASE WHEN A.installStatus IN (111, 779) THEN 1 ELSE 0 END)
         OVER (PARTITION BY A.LoanID ORDER BY A.InstallmentNumber
               ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) = 1        THEN 0
    -- Deny new: ALL installments are 684 and loan is returned
    WHEN A.installStatus = 684
     AND MIN(A.installStatus) OVER (PARTITION BY A.LoanID) = 684
     AND MAX(A.installStatus) OVER (PARTITION BY A.LoanID) = 684
     AND A.LoanStatus IN ('R')
     --AND COALESCE(InstallRealizedPayment, 0) = 0 
     AND A.InstallmentNumber = 1                                        THEN 1

    -- All voided
    WHEN A.installStatus = 115
         AND MIN(A.installStatus) OVER (PARTITION BY A.LoanID) = 115
         AND MAX(A.installStatus) OVER (PARTITION BY A.LoanID) = 115
         AND A.InstallmentNumber = 1                                        THEN 1

    -- Bad loan status + bad installment status (no recovery ahead)
    WHEN A.LoanStatus NOT IN ('D', 'P')
         AND A.installStatus NOT IN (111, 115, 779, 684)                   THEN 1

    -- Abandoned loan: first pending install after paid, loan returned
    WHEN A.installStatus = 684
     AND LAG(A.installStatus) OVER (PARTITION BY A.LoanID
                                    ORDER BY A.InstallmentNumber) IN (111, 779)
     AND A.LoanStatus NOT IN ('D', 'P', 'N')                                                THEN 1

    -- 786 & 825 on paid-off loan with no payment — terminal (money stopped coming in)
     WHEN A.installStatus IN (786, 825)
     AND A.LoanStatus = 'D'
     AND COALESCE(InstallRealizedPayment, 0) = 0
     AND LAG(COALESCE(InstallRealizedPayment, 0)) OVER (PARTITION BY A.LoanID 
                                                         ORDER BY A.InstallmentNumber) > 0
     THEN 1


    -- Good installment statuses
    WHEN A.installStatus = 111                                              THEN 0
    WHEN A.installStatus IN (115, 779)                                      THEN 0
    WHEN A.installStatus = 684                                              THEN 0
    ELSE 0
END AS isLoanDefault,

    /* *************** isInstallDefault *************** */
    CASE
        -- Deny new
        WHEN A.installStatus = 684
            AND MIN(A.installStatus) OVER (PARTITION BY A.LoanID) = 684
            AND MAX(A.installStatus) OVER (PARTITION BY A.LoanID) = 684
            AND A.LoanStatus IN ('R')
            --AND COALESCE(InstallRealizedPayment, 0) = 0
            AND A.InstallmentNumber = 1                                  THEN 1

        -- Bad status code (includes 825, 786, etc.)
        WHEN A.installStatus NOT IN (111, 779, 684)                         THEN 1

        -- Voided without prior payoff
        WHEN A.installStatus = 115
             AND LAG(A.installStatus) OVER (PARTITION BY A.LoanID
                                            ORDER BY A.InstallmentNumber) <> 111 THEN 1

        -- Abandoned loan: inherit from loan default
        WHEN A.installStatus = 684
        AND LAG(A.installStatus) OVER (PARTITION BY A.LoanID
                                    ORDER BY A.InstallmentNumber) IN (111, 779)
        AND A.LoanStatus NOT IN ('D', 'P', 'N')                                                THEN 1
        ELSE 0
    END AS isInstallDefault,
    
        /* *************** isDefaultBeforeFirst *************** */
        CASE
            WHEN A.installStatus = 115
                AND MIN(A.installStatus) OVER (PARTITION BY A.LoanID) = 115
                AND MAX(A.installStatus) OVER (PARTITION BY A.LoanID) = 115
                AND A.InstallmentNumber = 1                THEN 1
            ELSE 0
        END AS isAllVoided,
        /* *************** isDenyNew *************** */
        CASE
            WHEN A.installStatus = 684
                AND MIN(A.installStatus) OVER (PARTITION BY A.LoanID) = 684
                AND MAX(A.installStatus) OVER (PARTITION BY A.LoanID) = 684 
                AND A.LoanStatus IN ('R')
                AND COALESCE(att.hasPaymentAttempt, 0) = 0
                --AND  COALESCE(InstallRealizedPayment, 0) = 0 -- added a more restrictive check to contain installmet realzied payment
                AND A.InstallmentNumber = 1
                              THEN 1
            ELSE 0
        END AS isDenyNew,


    instA.ThirdPartyCollected,
    instA.PartialCollected,
    instA.InstallCollected,
    instA.EarlyCollected,
    instA.TransactionDate
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
LEFT JOIN #t_attempt att ON A.LoanID = att.LoanID -- Accurate DENY NEW Catcher
WHERE A.iPaymentMode = 144

/* =====================================================================
   SECTION 5: Trim Normal Installments (#t17a_final)
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
    AND (t17a.isRecentLoan = 0 OR t17a.InstallmentNumber = 1 OR COALESCE(t17a.InstallRealizedPayment, 0) > 0)

)
/* =====================================================================
   SECTION 6: Combined Final Table (Normal + Arrangement + 3rd Party)
   ===================================================================== */
-- Step 1: Stack just the installment-level data
DROP TABLE IF EXISTS #t17_stacked
SELECT 
    Application_ID, PortFolioID, LoanID,
    InstallmentNumber, InstallRealizedPayment, installStatus, iPaymentMode,
    TotalInstallsNumber, InstallmentDueDate,
    isRecentLoan, LoanPaidOffThisInstall, isLoanDefault, isInstallDefault,
    ThirdPartyCollected, PartialCollected, InstallCollected, EarlyCollected,
    isDenyNew, isAllVoided,
    0 AS isArrangementInstall, 0 AS is3rdPartyInstall,
    TransactionDate
INTO #t17_stacked
FROM #t17a_final

UNION ALL

SELECT
    a.Application_ID, a.PortFolioID, a.LoanID,
    b.InstallmentNumber, b.ArrangementRealizedPayment, b.installStatus, 679 AS iPaymentMode,
    a.TotalInstallsNumber, b.ArrangementDueDate AS InstallmentDueDate,
    0, 0, 0, 0,
    0, 0, 0, 0,
    0, 0,
    1, 0,
    TransactionDate
FROM (SELECT DISTINCT Application_ID, PortFolioID, LoanID, TotalInstallsNumber
      FROM #t17a_final) a
INNER JOIN #t5b b ON a.LoanID = b.LoanID

UNION ALL

SELECT
    a.Application_ID, a.PortFolioID, a.LoanID,
    c.InstallmentNumber, c.ThirdPartyRealizedPayment, c.installStatus, 685 AS iPaymentMode,
    a.TotalInstallsNumber, c.ThirdPartyDueDate AS InstallmentDueDate,
    0, 0, 0, 0,
    0, 0, 0, 0,
    0, 0,
    0, 1,
    TransactionDate
FROM (SELECT DISTINCT Application_ID, PortFolioID, LoanID, TotalInstallsNumber
      FROM #t17a_final) a
INNER JOIN #t5c c ON a.LoanID = c.LoanID

-- Step 2: Join loan-level columns once
DROP TABLE IF EXISTS #t17_combined
SELECT
    s.Application_ID, s.PortFolioID, s.LoanID,
    s.InstallmentNumber, s.InstallRealizedPayment, s.installStatus, s.iPaymentMode,
    s.TotalInstallsNumber, s.InstallmentDueDate,
    s.isRecentLoan, s.LoanPaidOffThisInstall, s.isLoanDefault, s.isInstallDefault,
    s.ThirdPartyCollected, s.PartialCollected, s.InstallCollected, s.EarlyCollected,
    s.isDenyNew, s.isAllVoided,
    s.isArrangementInstall, s.is3rdPartyInstall,
    t4.TotalRealizedPayment,
    t1.AppYear, t1.AppMonth, t1.AppWeek,
    t1.LoanStatus, t1.CustType, t1.Frequency,
    t1.OriginatedAmount, t1.OriginationDate,
    ROW_NUMBER() OVER (PARTITION BY s.LoanID 
                       ORDER BY s.iPaymentMode, s.InstallmentNumber) AS InstallmentNumberMFG,
    s.TransactionDate,
    YEAR(s.TransactionDate) AS TxYear,
    MONTH(s.TransactionDate) AS TxMonth
INTO #t17_combined
FROM #t17_stacked s
LEFT JOIN #t4 t4 ON s.LoanID = t4.LoanID AND s.Application_ID = t4.Application_ID
LEFT JOIN (SELECT DISTINCT Application_ID, PortFolioID, LoanID,
                  AppYear, AppMonth, AppWeek, LoanStatus, CustType,
                  Frequency, OriginatedAmount, OriginationDate
           FROM #t1) t1
    ON s.LoanID = t1.LoanID AND s.Application_ID = t1.Application_ID


/* ============================================================
 Monthly XPD by Appyear and Month
   Customer Type: ALL
   ============================================================ */
DROP TABLE IF EXISTS #monthly_summary
SELECT
    AppYear,
    AppMonth,
    COUNT(DISTINCT LoanID)                                                              AS CohortLoans,

    -- FPD
    SUM(CASE WHEN InstallmentNumber = 1 AND TotalInstallsNumber >= 1 THEN 1 ELSE 0 END)                          AS FPD_Denom,
    SUM(CASE WHEN InstallmentNumber = 1 AND TotalInstallsNumber >= 1 AND isLoanDefault = 1 THEN 1 ELSE 0 END) AS FPD_Num,
    CAST(SUM(CASE WHEN InstallmentNumber = 1 AND TotalInstallsNumber >= 1 AND isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN InstallmentNumber = 1 AND TotalInstallsNumber >= 1 THEN 1 ELSE 0 END), 0)         AS FPD_Rate,

    -- SPD
    SUM(CASE WHEN InstallmentNumber = 2 AND TotalInstallsNumber >= 2 THEN 1 ELSE 0 END)                          AS SPD_Denom,
    SUM(CASE WHEN InstallmentNumber = 2 AND TotalInstallsNumber >= 2 AND isLoanDefault = 1 THEN 1 ELSE 0 END) AS SPD_Num,
    CAST(SUM(CASE WHEN InstallmentNumber = 2 AND TotalInstallsNumber >= 2 AND isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN InstallmentNumber = 2 AND TotalInstallsNumber >= 2 THEN 1 ELSE 0 END), 0)         AS SPD_Rate,

    -- TPD
    SUM(CASE WHEN InstallmentNumber = 3 AND TotalInstallsNumber >= 3 THEN 1 ELSE 0 END)                          AS TPD_Denom,
    SUM(CASE WHEN InstallmentNumber = 3 AND TotalInstallsNumber >= 3 AND isLoanDefault = 1 THEN 1 ELSE 0 END) AS TPD_Num,
    CAST(SUM(CASE WHEN InstallmentNumber = 3 AND TotalInstallsNumber >= 3 AND isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN InstallmentNumber = 3 AND TotalInstallsNumber >= 3 THEN 1 ELSE 0 END), 0)         AS TPD_Rate,

    -- 4PD
    SUM(CASE WHEN InstallmentNumber = 4 AND TotalInstallsNumber >= 4 THEN 1 ELSE 0 END)                          AS PD4_Denom,
    SUM(CASE WHEN InstallmentNumber = 4 AND TotalInstallsNumber >= 4 AND isLoanDefault = 1 THEN 1 ELSE 0 END) AS PD4_Num,
    CAST(SUM(CASE WHEN InstallmentNumber = 4 AND TotalInstallsNumber >= 4 AND isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN InstallmentNumber = 4 AND TotalInstallsNumber >= 4 THEN 1 ELSE 0 END), 0)         AS PD4_Rate,

    -- 5PD
    SUM(CASE WHEN InstallmentNumber = 5 AND TotalInstallsNumber >= 5 THEN 1 ELSE 0 END)                          AS PD5_Denom,
    SUM(CASE WHEN InstallmentNumber = 5 AND TotalInstallsNumber >= 5 AND isLoanDefault = 1 THEN 1 ELSE 0 END) AS PD5_Num,
    CAST(SUM(CASE WHEN InstallmentNumber = 5 AND TotalInstallsNumber >= 5 AND isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN InstallmentNumber = 5 AND TotalInstallsNumber >= 5 THEN 1 ELSE 0 END), 0)         AS PD5_Rate

INTO #monthly_summary
FROM #t17_combined
WHERE isArrangementInstall = 0 AND is3rdPartyInstall = 0
GROUP BY AppYear, AppMonth
ORDER BY AppYear, AppMonth

DROP TABLE IF EXISTS #monthly_summary_new
SELECT
    m.AppYear,
    m.AppMonth,
    COUNT(DISTINCT m.LoanID)                                                              AS CohortLoans,
    orig.TotalOriginatedAmount,
    pay.TotalRealizedPayin,
    CAST(pay.TotalRealizedPayin AS FLOAT) 
        / NULLIF(orig.TotalOriginatedAmount, 0)                                           AS RealizedToOriginatedRatio,

    SUM(CASE WHEN m.InstallmentNumber = 1 AND m.TotalInstallsNumber >= 1 THEN 1 ELSE 0 END)                          AS FPD_Denom,
    SUM(CASE WHEN m.InstallmentNumber = 1 AND m.TotalInstallsNumber >= 1 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FPD_Num,
    CAST(SUM(CASE WHEN m.InstallmentNumber = 1 AND m.TotalInstallsNumber >= 1 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN m.InstallmentNumber = 1 AND m.TotalInstallsNumber >= 1 THEN 1 ELSE 0 END), 0)         AS FPD_Rate,

    SUM(CASE WHEN m.InstallmentNumber = 2 AND m.TotalInstallsNumber >= 2 THEN 1 ELSE 0 END)                          AS SPD_Denom,
    SUM(CASE WHEN m.InstallmentNumber = 2 AND m.TotalInstallsNumber >= 2 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS SPD_Num,
    CAST(SUM(CASE WHEN m.InstallmentNumber = 2 AND m.TotalInstallsNumber >= 2 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN m.InstallmentNumber = 2 AND m.TotalInstallsNumber >= 2 THEN 1 ELSE 0 END), 0)         AS SPD_Rate,

    SUM(CASE WHEN m.InstallmentNumber = 3 AND m.TotalInstallsNumber >= 3 THEN 1 ELSE 0 END)                          AS TPD_Denom,
    SUM(CASE WHEN m.InstallmentNumber = 3 AND m.TotalInstallsNumber >= 3 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS TPD_Num,
    CAST(SUM(CASE WHEN m.InstallmentNumber = 3 AND m.TotalInstallsNumber >= 3 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN m.InstallmentNumber = 3 AND m.TotalInstallsNumber >= 3 THEN 1 ELSE 0 END), 0)         AS TPD_Rate,

    SUM(CASE WHEN m.InstallmentNumber = 4 AND m.TotalInstallsNumber >= 4 THEN 1 ELSE 0 END)                          AS PD4_Denom,
    SUM(CASE WHEN m.InstallmentNumber = 4 AND m.TotalInstallsNumber >= 4 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS PD4_Num,
    CAST(SUM(CASE WHEN m.InstallmentNumber = 4 AND m.TotalInstallsNumber >= 4 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN m.InstallmentNumber = 4 AND m.TotalInstallsNumber >= 4 THEN 1 ELSE 0 END), 0)         AS PD4_Rate,

    SUM(CASE WHEN m.InstallmentNumber = 5 AND m.TotalInstallsNumber >= 5 THEN 1 ELSE 0 END)                          AS PD5_Denom,
    SUM(CASE WHEN m.InstallmentNumber = 5 AND m.TotalInstallsNumber >= 5 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS PD5_Num,
    CAST(SUM(CASE WHEN m.InstallmentNumber = 5 AND m.TotalInstallsNumber >= 5 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN m.InstallmentNumber = 5 AND m.TotalInstallsNumber >= 5 THEN 1 ELSE 0 END), 0)         AS PD5_Rate

INTO #monthly_summary_new
FROM #t17_combined m
LEFT JOIN (
    SELECT AppYear, AppMonth, SUM(TotalRealizedPayment) AS TotalRealizedPayin
    FROM (
        SELECT DISTINCT AppYear, AppMonth, LoanID, COALESCE(TotalRealizedPayment, 0) AS TotalRealizedPayment
        FROM #t17_combined
        WHERE CustType = 'NEW'
    ) loan_dedup
    GROUP BY AppYear, AppMonth
) pay ON pay.AppYear = m.AppYear AND pay.AppMonth = m.AppMonth
LEFT JOIN (
    SELECT YEAR(ApplicationDate) AS AppYear, MONTH(ApplicationDate) AS AppMonth,
           SUM(OriginatedAmount) AS TotalOriginatedAmount
    FROM (
        SELECT DISTINCT LoanID, ApplicationDate, OriginatedAmount, CustType
        FROM #t1
        WHERE CustType = 'NEW'
    ) loan_orig_dedup
    GROUP BY YEAR(ApplicationDate), MONTH(ApplicationDate)
) orig ON orig.AppYear = m.AppYear AND orig.AppMonth = m.AppMonth
WHERE m.isArrangementInstall = 0 AND m.is3rdPartyInstall = 0
  AND m.CustType = 'NEW'
GROUP BY m.AppYear, m.AppMonth, pay.TotalRealizedPayin, orig.TotalOriginatedAmount
ORDER BY m.AppYear, m.AppMonth





DROP TABLE IF EXISTS #monthly_summary_return
SELECT
    m.AppYear,
    m.AppMonth,
    COUNT(DISTINCT m.LoanID)                                                              AS CohortLoans,
    orig.TotalOriginatedAmount,
    pay.TotalRealizedPayin,
    CAST(pay.TotalRealizedPayin AS FLOAT) 
        / NULLIF(orig.TotalOriginatedAmount, 0)                                           AS RealizedToOriginatedRatio,

    SUM(CASE WHEN m.InstallmentNumber = 1 AND m.TotalInstallsNumber >= 1 THEN 1 ELSE 0 END)                          AS FPD_Denom,
    SUM(CASE WHEN m.InstallmentNumber = 1 AND m.TotalInstallsNumber >= 1 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FPD_Num,
    CAST(SUM(CASE WHEN m.InstallmentNumber = 1 AND m.TotalInstallsNumber >= 1 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN m.InstallmentNumber = 1 AND m.TotalInstallsNumber >= 1 THEN 1 ELSE 0 END), 0)         AS FPD_Rate,

    SUM(CASE WHEN m.InstallmentNumber = 2 AND m.TotalInstallsNumber >= 2 THEN 1 ELSE 0 END)                          AS SPD_Denom,
    SUM(CASE WHEN m.InstallmentNumber = 2 AND m.TotalInstallsNumber >= 2 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS SPD_Num,
    CAST(SUM(CASE WHEN m.InstallmentNumber = 2 AND m.TotalInstallsNumber >= 2 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN m.InstallmentNumber = 2 AND m.TotalInstallsNumber >= 2 THEN 1 ELSE 0 END), 0)         AS SPD_Rate,

    SUM(CASE WHEN m.InstallmentNumber = 3 AND m.TotalInstallsNumber >= 3 THEN 1 ELSE 0 END)                          AS TPD_Denom,
    SUM(CASE WHEN m.InstallmentNumber = 3 AND m.TotalInstallsNumber >= 3 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS TPD_Num,
    CAST(SUM(CASE WHEN m.InstallmentNumber = 3 AND m.TotalInstallsNumber >= 3 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN m.InstallmentNumber = 3 AND m.TotalInstallsNumber >= 3 THEN 1 ELSE 0 END), 0)         AS TPD_Rate,

    SUM(CASE WHEN m.InstallmentNumber = 4 AND m.TotalInstallsNumber >= 4 THEN 1 ELSE 0 END)                          AS PD4_Denom,
    SUM(CASE WHEN m.InstallmentNumber = 4 AND m.TotalInstallsNumber >= 4 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS PD4_Num,
    CAST(SUM(CASE WHEN m.InstallmentNumber = 4 AND m.TotalInstallsNumber >= 4 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN m.InstallmentNumber = 4 AND m.TotalInstallsNumber >= 4 THEN 1 ELSE 0 END), 0)         AS PD4_Rate,

    SUM(CASE WHEN m.InstallmentNumber = 5 AND m.TotalInstallsNumber >= 5 THEN 1 ELSE 0 END)                          AS PD5_Denom,
    SUM(CASE WHEN m.InstallmentNumber = 5 AND m.TotalInstallsNumber >= 5 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS PD5_Num,
    CAST(SUM(CASE WHEN m.InstallmentNumber = 5 AND m.TotalInstallsNumber >= 5 AND m.isLoanDefault = 1 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(CASE WHEN m.InstallmentNumber = 5 AND m.TotalInstallsNumber >= 5 THEN 1 ELSE 0 END), 0)         AS PD5_Rate

INTO #monthly_summary_return
FROM #t17_combined m
LEFT JOIN (
    SELECT AppYear, AppMonth, SUM(TotalRealizedPayment) AS TotalRealizedPayin
    FROM (
        SELECT DISTINCT AppYear, AppMonth, LoanID, COALESCE(TotalRealizedPayment, 0) AS TotalRealizedPayment
        FROM #t17_combined
        WHERE CustType = 'RETURN'
    ) loan_dedup
    GROUP BY AppYear, AppMonth
) pay ON pay.AppYear = m.AppYear AND pay.AppMonth = m.AppMonth
LEFT JOIN (
    SELECT YEAR(ApplicationDate) AS AppYear, MONTH(ApplicationDate) AS AppMonth,
           SUM(OriginatedAmount) AS TotalOriginatedAmount
    FROM (
        SELECT DISTINCT LoanID, ApplicationDate, OriginatedAmount, CustType
        FROM #t1
        WHERE CustType = 'RETURN'
    ) loan_orig_dedup
    GROUP BY YEAR(ApplicationDate), MONTH(ApplicationDate)
) orig ON orig.AppYear = m.AppYear AND orig.AppMonth = m.AppMonth
WHERE m.isArrangementInstall = 0 AND m.is3rdPartyInstall = 0
  AND m.CustType = 'RETURN'
GROUP BY m.AppYear, m.AppMonth, pay.TotalRealizedPayin, orig.TotalOriginatedAmount
ORDER BY m.AppYear, m.AppMonth




/* ============================================================
   2026 Monthly Realized Pay-In by TransactionDate Month
   Customer Type: NEW
   Grain: Payment transaction-level
   ============================================================ */

DROP TABLE IF EXISTS #monthly_summary_new_tx_2;

WITH loan_base AS (
    SELECT DISTINCT
        LoanID,
        Application_ID,
        PortfolioID,
        CustType
    FROM #t1
    WHERE CustType = 'NEW'
),

pay_txn AS (
    SELECT
        lb.LoanID,
        lb.Application_ID,
        lb.PortfolioID,
        lb.CustType,

        P.PaymentID,
        CAST(P.TransactionDate AS DATE) AS TransactionDate,
        YEAR(P.TransactionDate) AS TxYear,
        MONTH(P.TransactionDate) AS TxMonth,

        P.PaymentAmount
    FROM loan_base lb
    INNER JOIN LMSMaster..Payment P
        ON P.LoanID = lb.LoanID
    WHERE P.PaymentStatus = 'D'
      AND P.PaymentMode IN ('A','D','K','B')
      AND P.PaymentType IN ('Z','A','I','S','Q','X','~','3')
      AND P.InstallmentNumber >= 1
      AND P.TransactionDate >= '2026-01-01'
      AND P.TransactionDate <  '2027-01-01'
      AND P.TransactionDate IS NOT NULL
)

SELECT
    TxYear,
    TxMonth,

    COUNT(DISTINCT LoanID) AS PayingLoanCount,
    COUNT(DISTINCT PaymentID) AS PaymentTxnCount,

    SUM(PaymentAmount) AS TotalRealizedPayin

INTO #monthly_summary_new_tx_2
FROM pay_txn
GROUP BY
    TxYear,
    TxMonth
ORDER BY
    TxYear,
    TxMonth;


SELECT *
FROM #monthly_summary_new_tx_2
ORDER BY
    TxYear,
    TxMonth;


/* ============================================================
   2026 Monthly Realized Pay-In by TransactionDate Month
   Customer Type: Return
   Grain: Payment transaction-level
   ============================================================ */

DROP TABLE IF EXISTS #monthly_summary_return_tx;

WITH loan_base AS (
    SELECT DISTINCT
        LoanID,
        Application_ID,
        PortfolioID,
        CustType
    FROM #t1
    WHERE CustType = 'RETURN'
),

pay_txn AS (
    SELECT
        lb.LoanID,
        lb.Application_ID,
        lb.PortfolioID,
        lb.CustType,

        P.PaymentID,
        CAST(P.TransactionDate AS DATE) AS TransactionDate,
        YEAR(P.TransactionDate) AS TxYear,
        MONTH(P.TransactionDate) AS TxMonth,

        P.PaymentAmount
    FROM loan_base lb
    INNER JOIN LMSMaster..Payment P
        ON P.LoanID = lb.LoanID
    WHERE P.PaymentStatus = 'D'
      AND P.PaymentMode IN ('A','D','K','B')
      AND P.PaymentType IN ('Z','A','I','S','Q','X','~','3')
      AND P.InstallmentNumber >= 1
      AND P.TransactionDate >= '2026-01-01'
      AND P.TransactionDate <  '2027-01-01'
      AND P.TransactionDate IS NOT NULL
)

SELECT
    TxYear,
    TxMonth,

    COUNT(DISTINCT LoanID) AS PayingLoanCount,
    COUNT(DISTINCT PaymentID) AS PaymentTxnCount,

    SUM(PaymentAmount) AS TotalRealizedPayin

INTO #monthly_summary_return_tx
FROM pay_txn
GROUP BY
    TxYear,
    TxMonth
ORDER BY
    TxYear,
    TxMonth;


SELECT *
FROM #monthly_summary_return_tx
ORDER BY
    TxYear,
    TxMonth;