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
,	IIF(ptv.ActiveTruDat = 0, 998, ISNULL(cp.partnerid, 999)) as PartnerId
,	IIF(e1.TREXinMonth + e2.TREXinMonth + e3.TREXinMonth > 0, 1, 0) AS TREXFlag
,   isnull(e1.pets,0) + isnull(e2.pets,0) + isnull(e3.pets,0) AS Pets90
into #ClinicMonth2
from #FullList2
	left join dw.ClinicPartner cp with (nolock) on (#FullList2.clinicid = cp.ClinicId) AND cp.EndDate IS NULL
	left join dw.PartnerTraitV ptv with (nolock) on (cp.PartnerId = ptv.PartnerID)
    left join #FullList2 e1 on e1.datekey = #FullList2.datekey and e1.clinicID = #FullList2.clinicID
    left join #FullList2 e2 on e2.datekey = dateadd(m,-1,#FullList2.datekey) and e2.clinicID = #FullList2.clinicID
    left join #FullList2 e3 on e3.datekey = dateadd(m,-2,#FullList2.datekey) and e3.clinicID = #FullList2.clinicID
--order by #FullList2.clinicid, #FullList2.datekey

----------------- FINAL SELECT STATEMENT ------------------
--if object_id('tempdb..#TREXFinal','u') is not null
--    drop table #TREXFinal
select
    cm.datekey
,	cm.PartnerId
,   sum(iif(cm.Pets90 >= 1,1,0)) AS ActiveHosps
,	sum(iif(cm.Pets90 >= 1, cm.TREXFlag, 0)) AS trxhosps
--into #TREXFinal
from #ClinicMonth2 cm
group by cm.datekey
,	cm.PartnerId
order by cm.PartnerId, cm.datekey


