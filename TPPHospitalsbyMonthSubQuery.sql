--------------A clinic's Enrs in a Mo    (154437 rows, all months, all hosps with enrollments) -----------------
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


-----------------A clinic's TPP Implement Mo  (2.6 million rows, all months, TPP only)-------------------
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
,	IIF(ptv.ActiveTruDat = 0, 998, ISNULL(cp.partnerid, 999)) as PartnerId
,	IIF(e1.TPPinMonth + e2.TPPinMonth + e3.TPPinMonth > 0, 1, 0) AS TPPFlag
,   isnull(e1.pets,0) + isnull(e2.pets,0) + isnull(e3.pets,0) AS Pets90
into #clinicmonth1
from #FullList1
	left join dw.ClinicPartner cp with (nolock) on (#FullList1.clinicid = cp.ClinicId) AND cp.EndDate IS NULL
	left join dw.PartnerTraitV ptv with (nolock) on (cp.PartnerId = ptv.PartnerID)
    left join #FullList1 e1 on e1.datekey = #FullList1.datekey and e1.clinicID = #FullList1.clinicID
    left join #FullList1 e2 on e2.datekey = dateadd(m,-1,#FullList1.datekey) and e2.clinicID = #FullList1.clinicID
    left join #FullList1 e3 on e3.datekey = dateadd(m,-2,#FullList1.datekey) and e3.clinicID = #FullList1.clinicID
--order by #FullList1.clinicid, #FullList1.datekey

----------------- FINAL SELECT STATEMENT ------------------
--if object_id('tempdb..#TPPFinal','u') is not null
--    drop table #TPPFinal
select
    cm.datekey
,	cm.PartnerId
,   sum(iif(cm.Pets90 >= 1,1,0)) AS ActiveHosps
,	sum(iif(cm.Pets90 >= 1, cm.TPPFlag, 0)) AS TPPHosps
--into #TPPFinal
from #clinicmonth1 cm
group by cm.datekey
,	cm.PartnerId



