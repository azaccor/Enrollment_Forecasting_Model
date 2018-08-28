
---------------- DTC Campaigns SubQuery ------------------
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
from fct.EnrTableV b with (nolock)
left join dw.PartnerTraitV ptv with (nolock) on (b.partnerid = ptv.partnerid)
left join sse.DTCCampaigns dtc1 with (nolock) on (b.PostalCode = dtc1.zipcode)
left join sse.DTCCampaigns dtc2 with (nolock) on (LEFT(b.PostalCode, 3) = dtc2.FSA)
where b.EnrollDate >= '1/1/2016'
group by DATEFROMPARTS(year(b.enrolldate), month(b.enrolldate), 1)
, IIF(ptv.ActiveTruDat = 0, 998, ISNULL(b.partnerid, 999))


