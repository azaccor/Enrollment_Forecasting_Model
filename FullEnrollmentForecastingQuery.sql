/************************************************************************/
/*							Beginning of Query							*/
/*						TPP Hospitals Subsection						*/
/************************************************************************/

--------------A clinic's Enrs in a Mo  -----------------
if object_id('tempdb..#Enrs1','u') is not null
    drop table #Enrs1
select
    dateadd(d,-day(b.enrolldateonly)+1,b.enrolldateonly) AS EnrMo
,   b.clinicID
,   count(distinct b.clinicID) AS Hosps
,   count(b.ppid) AS Pets
into #Enrs1
from fct.enrtablev b with (nolock)
where b.clinicID is not null and b.enrolldate is not null and b.paidbycorp = 0
and b.EnrollDate >= '9/1/2015' -- 3 months preceding date of interest
group by
    dateadd(d,-day(b.enrolldateonly)+1,b.enrolldateonly)
,   b.clinicID


-----------------A clinic's TPP Implement Mo  -------------------
if object_id('tempdb..#TPPHosps', 'u') IS NOT NULL
	drop table #TPPHosps
select 
	datefromparts(year(ct.signupimplementationdate), month(ct.signupimplementationdate), 1) as TPPStartMo
,	DATEFROMPARTS(year(cv.datekey), month(cv.datekey), 1) as KeyMo
,	ct.clinicid
into #TPPHosps
from sse.clinictrait ct
cross apply dim.CalendarV cv
where cv.datekey >= '9/1/2015' and cv.datekey <= getdate() -- 3 months preceding date of interest
AND ct.SignUpImplementationDate IS NOT NULL
--order by ct.ClinicId, cv.datekey


-----------------MonthBase1 Table   (all months all clinics, no exclusions) -----------------
if object_id('tempdb..#MonthBase1','u') is not null
    drop table #MonthBase1
select
    distinct datekey = dateadd(d,-day(cv.datekey)+1,convert(date,datekey))
,   b.clinicID
into #MonthBase1
from dim.calendarv cv with (nolock)
	cross apply fct.enrtablev b with (nolock)
where cv.datekey >= '1/1/2016' and cv.datekey <= getdate()


----------------All the months where a hospital is TPP  (Same as MonthBase1)-----------------
if object_id('tempdb..#TPPinMonth', 'u') IS NOT NULL
	drop table #TPPinMonth
Select mb.datekey
,	 mb.clinicid
,	 ISNULL(IIF(tpph.KeyMo >= tpph.TPPStartMo, 1, 0), 0) as TPPinMonth
into #TPPinMonth
from #MonthBase1 mb
left join #TPPHosps tpph with (nolock) on (mb.datekey = tpph.KeyMo AND mb.clinicid = tpph.ClinicId)
group by mb.datekey
,	 mb.clinicid
,	 ISNULL(IIF(tpph.KeyMo >= tpph.TPPStartMo, 1, 0), 0)
--order by mb.clinicid, mb.datekey


-----------Combining Enrollments with TPP in Month Flag (all months, all hosps) --------------
if object_id('tempdb..#FullList1', 'u') IS NOT NULL
	drop table #FullList1
select TM.datekey
,	TM.clinicid
,	ISNULL(E.Hosps, 0) AS Hosps
,	ISNULL(E.Pets, 0) AS Pets
,	ISNULL(TM.TPPinMonth, 0) AS TPPinMonth
into #FullList1
from #TPPinMonth TM
full outer join #Enrs1 E with (nolock) on (TM.clinicid = E.clinicid AND TM.datekey = E.EnrMo)
group by TM.datekey
,	TM.ClinicId
,	ISNULL(E.Hosps, 0)
,	ISNULL(E.Pets, 0)
,	ISNULL(TM.TPPinMonth, 0)
--order by TM.clinicid, TM.datekey
  

-----------------Active and TPP by Month   (same as above) --------------------  
if object_id('tempdb..#clinicmonth1','u') is not null
    drop table #clinicmonth1
select
    #FullList1.datekey
,   #FullList1.clinicid
,	cp.PartnerId
,	IIF(e1.TPPinMonth + e2.TPPinMonth + e3.TPPinMonth > 0, 1, 0) AS TPPFlag
,   isnull(e1.pets,0) + isnull(e2.pets,0) + isnull(e3.pets,0) AS Pets90
into #clinicmonth1
from #FullList1
	left join dw.ClinicPartner cp with (nolock) on (#FullList1.clinicid = cp.ClinicId) AND cp.EndDate IS NULL
    left join #FullList1 e1 on e1.datekey = #FullList1.datekey and e1.clinicID = #FullList1.clinicID
    left join #FullList1 e2 on e2.datekey = dateadd(m,-1,#FullList1.datekey) and e2.clinicID = #FullList1.clinicID
    left join #FullList1 e3 on e3.datekey = dateadd(m,-2,#FullList1.datekey) and e3.clinicID = #FullList1.clinicID
order by #FullList1.clinicid, #FullList1.datekey

----------------- FINAL SELECT STATEMENT ------------------
if object_id('tempdb..#TPPFinal','u') is not null
    drop table #TPPFinal
select
    cm.datekey
,	cm.PartnerId
,   sum(iif(cm.Pets90 >= 1,1,0)) AS ActiveHosps
,	sum(iif(cm.Pets90 >= 1, cm.TPPFlag, 0)) AS TPPHosps
into #TPPFinal
from #clinicmonth1 cm
group by cm.datekey
,	cm.PartnerId



/************************************************************************/
/*								Second Section							*/
/*						TREX Hospitals Subsection						*/
/************************************************************************/

--------------A clinic's Enrs in a Mo-----------------
if object_id('tempdb..#Enrs2','u') is not null
    drop table #Enrs2
select
    dateadd(d,-day(b.enrolldateonly)+1,b.enrolldateonly) AS EnrMo
,   b.clinicID
,   count(distinct b.clinicID) AS Hosps
,   count(b.ppid) AS Pets
into #Enrs2
from fct.enrtablev b with (nolock)
where b.clinicID is not null and b.enrolldate is not null and b.paidbycorp = 0
and b.EnrollDate >= '9/1/2015' -- 3 months preceding date of interest
group by
    dateadd(d,-day(b.enrolldateonly)+1,b.enrolldateonly)
,   b.clinicID


-----------------A clinic's TREX Live Mo-------------------
if object_id('tempdb..#trxhosps', 'u') IS NOT NULL
	drop table #trxhosps
select 
	datefromparts(year(ct.TrexLiveDate), month(ct.TrexLiveDate), 1) as TREXStartMo
,	DATEFROMPARTS(year(cv.datekey), month(cv.datekey), 1) as KeyMo
,	ct.clinicid
into #trxhosps
from sse.clinictrait ct
cross apply dim.CalendarV cv
where cv.datekey >= '9/1/2015' and cv.datekey <= getdate() -- 3 months preceding date of interest
AND ct.TrexLiveDate IS NOT NULL
--order by ct.ClinicId, cv.datekey


-----------------MonthBase2 Table-----------------
if object_id('tempdb..#MonthBase2','u') is not null
    drop table #MonthBase2
select
    distinct datekey = dateadd(d,-day(cv.datekey)+1,convert(date,datekey))
,   b.clinicID
into #MonthBase2
from dim.calendarv cv with (nolock)
	cross apply fct.enrtablev b with (nolock)
where cv.datekey >= '1/1/2016' and cv.datekey <= getdate()


----------------All the months where a hospital is TREX-------------------
if object_id('tempdb..#TREXinMonth', 'u') IS NOT NULL
	drop table #TREXinMonth
Select mb.datekey
,	 mb.clinicid
,	 ISNULL(IIF(trxh.KeyMo >= trxh.TREXStartMo, 1, 0), 0) as TREXinMonth
into #TREXinMonth
from #MonthBase2 mb
left join #trxhosps trxh with (nolock) on (mb.datekey = trxh.KeyMo AND mb.clinicid = trxh.ClinicId)
group by mb.datekey
,	 mb.clinicid
,	 ISNULL(IIF(trxh.KeyMo >= trxh.TREXStartMo, 1, 0), 0)
--order by mb.clinicid, mb.datekey


-----------Combining Enrollments with TREX in Month Flag--------------
if object_id('tempdb..#FullList2', 'u') IS NOT NULL
	drop table #FullList2
select TM.datekey
,	TM.clinicid
,	ISNULL(E.Hosps, 0) AS Hosps
,	ISNULL(E.Pets, 0) AS Pets
,	ISNULL(TM.TREXinMonth, 0) AS TREXinMonth
into #FullList2
from #TREXinMonth TM
full outer join #Enrs2 E with (nolock) on (TM.clinicid = E.clinicid AND TM.datekey = E.EnrMo)
group by TM.datekey
,	TM.ClinicId
,	ISNULL(E.Hosps, 0)
,	ISNULL(E.Pets, 0)
,	ISNULL(TM.TREXinMonth, 0)
--order by TM.clinicid, TM.datekey
  

-----------------Active and TREX by Month--------------------  
if object_id('tempdb..#ClinicMonth2','u') is not null
    drop table #ClinicMonth2
select
    #FullList2.datekey
,   #FullList2.clinicid
,	cp.PartnerId
,	IIF(e1.TREXinMonth + e2.TREXinMonth + e3.TREXinMonth > 0, 1, 0) AS TREXFlag
,   isnull(e1.pets,0) + isnull(e2.pets,0) + isnull(e3.pets,0) AS Pets90
into #ClinicMonth2
from #FullList2
	left join dw.ClinicPartner cp with (nolock) on (#FullList2.clinicid = cp.ClinicId) AND cp.EndDate IS NULL
    left join #FullList2 e1 on e1.datekey = #FullList2.datekey and e1.clinicID = #FullList2.clinicID
    left join #FullList2 e2 on e2.datekey = dateadd(m,-1,#FullList2.datekey) and e2.clinicID = #FullList2.clinicID
    left join #FullList2 e3 on e3.datekey = dateadd(m,-2,#FullList2.datekey) and e3.clinicID = #FullList2.clinicID
order by #FullList2.clinicid, #FullList2.datekey

----------------- FINAL SELECT STATEMENT ------------------
if object_id('tempdb..#TREXFinal','u') is not null
    drop table #TREXFinal
select
    cm.datekey
,	cm.PartnerId
,   sum(iif(cm.Pets90 >= 1,1,0)) AS ActiveHosps
,	sum(iif(cm.Pets90 >= 1, cm.TREXFlag, 0)) AS trxhosps
into #TREXFinal
from #ClinicMonth2 cm
group by cm.datekey
,	cm.PartnerId
order by cm.PartnerId, cm.datekey



/************************************************************************/
/*							Third Section								*/
/*						DTC Campaigns Subsection						*/
/************************************************************************/

if object_id('tempdb..#DTCFinal','u') is not null
    drop table #DTCFinal
select DATEFROMPARTS(year(b.enrolldate), month(b.enrolldate), 1) as EnrMo
, IIF(ptv.ActiveTruDat = 0, 998, ISNULL(b.partnerid, 999)) as PartnerId
, IIF(
	SUM(
		IIF(
			ISNULL(dtc1.Medium, dtc2.Medium) = 'TV' 
			AND b.enrolldate >= ISNULL(dtc1.StartDate, dtc2.StartDate) 
			AND b.enrolldate <= DATEADD(DAY, 14, ISNULL(dtc1.EndDate, dtc2.EndDate)), 1, 0)) >= 1, 1, 0) as TVFlag
, IIF(
	SUM(
		IIF(
			ISNULL(dtc1.Medium, dtc2.Medium) = 'Radio' 
			AND b.enrolldate >= ISNULL(dtc1.StartDate, dtc2.StartDate) 
			AND b.enrolldate <= DATEADD(DAY, 14, ISNULL(dtc1.EndDate, dtc2.EndDate)), 1, 0)) >= 1, 1, 0) as RadioFlag
into #DTCFinal
from fct.EnrTableV b with (nolock)
left join dw.PartnerTraitV ptv with (nolock) on (b.partnerid = ptv.partnerid)
left join sse.DTCCampaigns dtc1 with (nolock) on (b.PostalCode = dtc1.zipcode)
left join sse.DTCCampaigns dtc2 with (nolock) on (LEFT(b.PostalCode, 3) = dtc2.FSA)
where b.EnrollDate >= '1/1/2016'
group by DATEFROMPARTS(year(b.enrolldate), month(b.enrolldate), 1)
, IIF(ptv.ActiveTruDat = 0, 998, ISNULL(b.partnerid, 999))





/************************************************************************/
/*							FINAL SECTION								*/
/************************************************************************/

SELECT MONTH(b.EnrollDate) AS 'EnrMo'
, YEAR(b.EnrollDate)-2015 AS 'EnrYr'
, IIF(ptv.ActiveTruDat = 0, 998, ISNULL(cd.TrueCreditPartner, 999)) AS 'PartnerId'
, TPP.TPPHosps AS 'TPPHospitals'
, TREX.trxhosps AS 'TrexHospitals'
, TPP.ActiveHosps AS 'ActiveHosps'
, IIF(TPP.ActiveHosps = 0, 0, CAST(TPP.TPPHosps AS DECIMAL(5))/CAST(TPP.ActiveHosps AS DECIMAL(5))) AS 'TPPRatio'
, IIF(TREX.ActiveHosps = 0, 0, CAST(TREX.trxhosps AS DECIMAL(5))/CAST(TREX.ActiveHosps AS DECIMAL(5))) AS 'TrexRatio'
, ISNULL(DTC.RadioFlag, 0) AS 'RadioFlag'
, ISNULL(DTC.TVFlag, 0) AS 'TVFlag'
, COUNT(DISTINCT b.ppid) AS 'Enrolls'
FROM fct.EnrTableV b with (nolock)
LEFT JOIN sse.TPCreditDriverV cd with (nolock) on (b.ppid = cd.ppid)
LEFT JOIN dw.PartnerTraitV ptv with (nolock) on (cd.TrueCreditPartner = ptv.PartnerID)
INNER JOIN #TPPFinal TPP on (DATEFROMPARTS(YEAR(b.enrolldate), MONTH(b.enrolldate), 1) = TPP.datekey AND cd.TrueCreditPartner = TPP.PartnerId)
INNER JOIN #TREXFinal TREX on (DATEFROMPARTS(YEAR(b.enrolldate), MONTH(b.enrolldate), 1) = TREX.datekey AND cd.TrueCreditPartner = TREX.PartnerId)
LEFT JOIN #DTCFinal DTC  on (DATEFROMPARTS(YEAR(b.enrolldate), MONTH(b.enrolldate), 1) = DTC.EnrMo AND cd.TrueCreditPartner = DTC.PartnerId)
WHERE b.EnrollDate >= '1/1/2016'
AND b.EnrollDate <= GETDATE()
GROUP BY MONTH(b.EnrollDate)
, YEAR(b.EnrollDate)-2015
, IIF(ptv.ActiveTruDat = 0, 998, ISNULL(cd.TrueCreditPartner, 999))
, TPP.TPPHosps
, TREX.trxhosps
, TPP.ActiveHosps
, IIF(TPP.ActiveHosps = 0, 0, CAST(TPP.TPPHosps AS DECIMAL(5))/CAST(TPP.ActiveHosps AS DECIMAL(5)))
, IIF(TREX.ActiveHosps = 0, 0, CAST(TREX.trxhosps AS DECIMAL(5))/CAST(TREX.ActiveHosps AS DECIMAL(5)))
, ISNULL(DTC.RadioFlag, 0)
, ISNULL(DTC.TVFlag, 0)
ORDER BY IIF(ptv.ActiveTruDat = 0, 998, ISNULL(cd.TrueCreditPartner, 999))
, YEAR(b.EnrollDate)-2015
, MONTH(b.EnrollDate)
