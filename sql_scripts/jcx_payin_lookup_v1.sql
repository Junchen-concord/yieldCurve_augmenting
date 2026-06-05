/* =====================================================================
   jcx_payin_lookup_v1.sql
   ---------------------------------------------------------------------
   Loan-level extract for the day-zero historical payin lookup.

   One row per (Application_ID, PortFolioID, LoanID). No installment
   stacking, no arrangement / 3rd-party stream — just the inputs needed
   to look up "what did similar loans historically pay in?" by:
       (DM_Band_Name, CM_Band_Name, CustType, PortFolioID,
        AppMonth, AppWeek, Frequency_group3)

   Read via execute_sql_and_read_temp_table on '#t_lookup'.

   Output columns:
     Application_ID, PortFolioID, LoanID, LoanStatus,
     CustType, AppYear, AppMonth, AppWeek, Frequency,
     DM_Band_Name, CM_Band_Name,
     OriginatedAmount, OriginationDate, TotalRealizedPayment

   Patterns mirror jcx_raw_harvey_v14.sql exactly:
     - #t1       : Application + Loans + VW_ApplicationDump (Frequency, CustType)
                   - dedup'd here to ONE ROW PER LOAN (no installment fanout).
     - #t4       : SUM Payment.PaymentAmount where PaymentStatus='D'
                   - same filter set as v14 (PaymentMode/PaymentType list).
     - #sa_bands : MAX(DM_Band_Name), MAX(CM_Band_Name) from
                   QlikDB..ScoredApplications, dedup'd per (App_ID, Portfolio).

   Notes / assumptions (please review):
     1) ApplicationDate filter starts 2023-01-01 (matches v14 working window).
     2) LoanStatus filter excludes 'V','W','G','K' (matches v14).
     3) Maturity cutoff (OriginationDate <= 2025-11-30) is applied in the
        notebook, NOT here, so the extract can be re-split without re-running.
     4) Frequency 3-group collapse (W / B+S→B / M) happens in the notebook.
   ===================================================================== */
USE LMSMaster;


/* =====================================================================
   SECTION 1: Loan base — ONE ROW PER LOAN
   ===================================================================== */
DROP TABLE IF EXISTS #t1;
SELECT
    A.Application_ID,
    A.PortFolioID,
    L.LoanID,
    L.LoanStatus,
    YEAR(A.ApplicationDate)              AS AppYear,
    MONTH(A.ApplicationDate)             AS AppMonth,
    DATEPART(WEEK, A.ApplicationDate)    AS AppWeek,
    VW.Frequency,
    CASE
        WHEN (A.ApplicationSteps NOT LIKE '%R%' AND A.ApplicationSteps NOT LIKE '%O%')
        THEN 'NEW' ELSE 'RETURN'
    END                                  AS CustType,
    CASE
        WHEN L.LoanStatus NOT IN ('V','W','G','K')
        THEN L.OriginatedAmount ELSE NULL
    END                                  AS OriginatedAmount,
    CAST(L.OriginationDate AS DATE)      AS OriginationDate
INTO #t1
FROM LMSMaster..Application AS A
LEFT JOIN Loans L
    ON A.Application_ID = L.ApplicationID
   AND A.PortFolioID    = L.PortFolioID
LEFT JOIN LMS_Logs..VW_ApplicationDump VW
    ON A.APPGUID = VW.APPGUID
WHERE A.ApplicationDate >= '2023-01-01'
  AND L.LoanStatus NOT IN ('V','W','G','K');


/* =====================================================================
   SECTION 2: Loan-level Total Realized Payments
   (same PaymentMode / PaymentType / PaymentStatus filter set as v14 #t4)
   ===================================================================== */
DROP TABLE IF EXISTS #t4;
SELECT
    A.Application_ID, A.PortFolioID, A.LoanID,
    SUM(CASE WHEN P.PaymentStatus = 'D' THEN P.PaymentAmount ELSE 0 END)
        AS TotalRealizedPayment
INTO #t4
FROM (SELECT DISTINCT Application_ID, PortFolioID, LoanID FROM #t1) A
INNER JOIN Payment P
    ON A.LoanID = P.LoanID
   AND P.PaymentMode IN ('A','D','K','B')
   AND P.PaymentType IN ('Z','A','I','S','Q','X','~','3')
   AND P.InstallmentNumber >= 1
   AND P.PaymentStatus = 'D'
GROUP BY A.Application_ID, A.PortFolioID, A.LoanID;


/* =====================================================================
   SECTION 3: Underwriting Risk Bands (DM / CM)
   Pulls DM_Band_Name and CM_Band_Name from QlikDB..ScoredApplications.
   Deduped per (Application_ID, PortFolioID) via MAX, identical to v14.
   ===================================================================== */
DROP TABLE IF EXISTS #sa_bands;
SELECT
    Application_ID,
    PortFolioID,
    MAX(DM_Band_Name) AS DM_Band_Name,
    MAX(CM_Band_Name) AS CM_Band_Name
INTO #sa_bands
FROM QlikDB..ScoredApplications
WHERE Application_ID IS NOT NULL
GROUP BY Application_ID, PortFolioID;


/* =====================================================================
   SECTION 4: Final one-row-per-loan lookup table (#t_lookup)
   ===================================================================== */
DROP TABLE IF EXISTS #t_lookup;
SELECT
    t1.Application_ID,
    t1.PortFolioID,
    t1.LoanID,
    t1.LoanStatus,
    t1.CustType,
    t1.AppYear,
    t1.AppMonth,
    t1.AppWeek,
    t1.Frequency,
    sb.DM_Band_Name,
    sb.CM_Band_Name,
    t1.OriginatedAmount,
    t1.OriginationDate,
    ISNULL(t4.TotalRealizedPayment, 0) AS TotalRealizedPayment
INTO #t_lookup
FROM #t1 t1
LEFT JOIN #t4 t4
    ON t1.LoanID         = t4.LoanID
   AND t1.Application_ID = t4.Application_ID
   AND t1.PortFolioID    = t4.PortFolioID
LEFT JOIN #sa_bands sb
    ON t1.Application_ID = sb.Application_ID
   AND t1.PortFolioID    = sb.PortFolioID;
