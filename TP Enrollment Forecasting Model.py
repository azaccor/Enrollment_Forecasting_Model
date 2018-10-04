
# coding: utf-8

# ## Predicting Conversions from Web Quotes

# In[61]:


import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import random

from sqlalchemy import create_engine
import pyodbc
import urllib

import statsmodels.api as sm
import statsmodels.tools.tools as smt
from statsmodels.genmod.families import NegativeBinomial


# Import the data from SQL Server

# In[27]:


TempTPP1 = """
--------------A clinic's Enrs in a Mo  -----------------
if object_id('tempdb..##Enrs1','u') is not null
    drop table ##Enrs1
select
    dateadd(d,-day(b.enrolldateonly)+1,b.enrolldateonly) AS EnrMo
,   b.clinicID
,   count(distinct b.clinicID) AS Hosps
,   count(b.ppid) AS Pets
into ##Enrs1
from fct.enrtablev b with (nolock)
where b.clinicID is not null and b.enrolldate is not null and b.paidbycorp = 0
and b.EnrollDate >= '6/1/2015' -- 3 months preceding date of interest
group by
    dateadd(d,-day(b.enrolldateonly)+1,b.enrolldateonly)
,   b.clinicID
"""

TempTPP2 = """
-----------------A clinic's TPP Implement Mo  -------------------
if object_id('tempdb..##TPPHosps', 'u') IS NOT NULL
	drop table ##TPPHosps
select 
	datefromparts(year(ct.signupimplementationdate), month(ct.signupimplementationdate), 1) as TPPStartMo
,	DATEFROMPARTS(year(cv.datekey), month(cv.datekey), 1) as KeyMo
,	ct.clinicid
into ##TPPHosps
from sse.clinictrait ct
cross apply dim.CalendarV cv
where cv.datekey >= '6/1/2015' and cv.datekey <= getdate() -- 3 months preceding date of interest
AND ct.SignUpImplementationDate IS NOT NULL
--order by ct.ClinicId, cv.datekey
"""

TempTPP3 = """
-----------------MonthBase1 Table   (all months all clinics, no exclusions) -----------------
if object_id('tempdb..##MonthBase1','u') is not null
    drop table ##MonthBase1
select
    distinct datekey = dateadd(d,-day(cv.datekey)+1,convert(date,datekey))
,   b.clinicID
into ##MonthBase1
from dim.calendarv cv with (nolock)
	cross apply fct.enrtablev b with (nolock)
where cv.datekey >= '6/1/2015' and cv.datekey <= getdate()
"""

TempTPP4 = """
----------------All the months where a hospital is TPP  (Same as MonthBase1)-----------------
if object_id('tempdb..##TPPinMonth', 'u') IS NOT NULL
	drop table ##TPPinMonth
Select mb.datekey
,	 mb.clinicid
,	 ISNULL(IIF(tpph.KeyMo >= tpph.TPPStartMo, 1, 0), 0) as TPPinMonth
into ##TPPinMonth
from ##MonthBase1 mb
left join ##TPPHosps tpph with (nolock) on (mb.datekey = tpph.KeyMo AND mb.clinicid = tpph.ClinicId)
group by mb.datekey
,	 mb.clinicid
,	 ISNULL(IIF(tpph.KeyMo >= tpph.TPPStartMo, 1, 0), 0)
--order by mb.clinicid, mb.datekey
"""

TempTPP5 = """
-----------Combining Enrollments with TPP in Month Flag (all months, all hosps) --------------
if object_id('tempdb..##FullList1', 'u') IS NOT NULL
	drop table ##FullList1
select TM.datekey
,	TM.clinicid
,	ISNULL(E.Hosps, 0) AS Hosps
,	ISNULL(E.Pets, 0) AS Pets
,	ISNULL(TM.TPPinMonth, 0) AS TPPinMonth
into ##FullList1
from ##TPPinMonth TM
full outer join ##Enrs1 E with (nolock) on (TM.clinicid = E.clinicid AND TM.datekey = E.EnrMo)
group by TM.datekey
,	TM.ClinicId
,	ISNULL(E.Hosps, 0)
,	ISNULL(E.Pets, 0)
,	ISNULL(TM.TPPinMonth, 0)
--order by TM.clinicid, TM.datekey
"""

TempTPP6 = """
-----------------Active and TPP by Month   (same as above) --------------------  
if object_id('tempdb..##clinicmonth1','u') is not null
    drop table ##clinicmonth1
select
    ##FullList1.datekey
,   ##FullList1.clinicid
,	cp.PartnerId
,	IIF(e1.TPPinMonth + e2.TPPinMonth + e3.TPPinMonth > 0, 1, 0) AS TPPFlag
,   isnull(e1.pets,0) + isnull(e2.pets,0) + isnull(e3.pets,0) AS Pets90
into ##clinicmonth1
from ##FullList1
	left join dw.ClinicPartner cp with (nolock) on (##FullList1.clinicid = cp.ClinicId) AND cp.EndDate IS NULL
    left join ##FullList1 e1 on e1.datekey = ##FullList1.datekey and e1.clinicID = ##FullList1.clinicID
    left join ##FullList1 e2 on e2.datekey = dateadd(m,-1,##FullList1.datekey) and e2.clinicID = ##FullList1.clinicID
    left join ##FullList1 e3 on e3.datekey = dateadd(m,-2,##FullList1.datekey) and e3.clinicID = ##FullList1.clinicID
order by ##FullList1.clinicid, ##FullList1.datekey
"""

TempTPP7 = """
----------------- FINAL SELECT STATEMENT ------------------
if object_id('tempdb..##TPPFinal','u') is not null
    drop table ##TPPFinal
select
    cm.datekey
,	cm.PartnerId
,   sum(iif(cm.Pets90 >= 1,1,0)) AS ActiveHosps
,	sum(iif(cm.Pets90 >= 1, cm.TPPFlag, 0)) AS TPPHosps
into ##TPPFinal
from ##clinicmonth1 cm
group by cm.datekey
,	cm.PartnerId
"""


TempTREX1 = """
--------------A clinic's Enrs in a Mo-----------------
if object_id('tempdb..##Enrs2','u') is not null
    drop table ##Enrs2
select
    dateadd(d,-day(b.enrolldateonly)+1,b.enrolldateonly) AS EnrMo
,   b.clinicID
,   count(distinct b.clinicID) AS Hosps
,   count(b.ppid) AS Pets
into ##Enrs2
from fct.enrtablev b with (nolock)
where b.clinicID is not null and b.enrolldate is not null and b.paidbycorp = 0
and b.EnrollDate >= '6/1/2015' -- 3 months preceding date of interest
group by
    dateadd(d,-day(b.enrolldateonly)+1,b.enrolldateonly)
,   b.clinicID
"""

TempTREX2 = """
-----------------A clinic's TREX Live Mo-------------------
if object_id('tempdb..##trxhosps', 'u') IS NOT NULL
	drop table ##trxhosps
select 
	datefromparts(year(ct.TrexLiveDate), month(ct.TrexLiveDate), 1) as TREXStartMo
,	DATEFROMPARTS(year(cv.datekey), month(cv.datekey), 1) as KeyMo
,	ct.clinicid
into ##trxhosps
from sse.clinictrait ct
cross apply dim.CalendarV cv
where cv.datekey >= '6/1/2015' and cv.datekey <= getdate() -- 3 months preceding date of interest
AND ct.TrexLiveDate IS NOT NULL
--order by ct.ClinicId, cv.datekey
"""

TempTREX3 = """
-----------------MonthBase2 Table-----------------
if object_id('tempdb..##MonthBase2','u') is not null
    drop table ##MonthBase2
select
    distinct datekey = dateadd(d,-day(cv.datekey)+1,convert(date,datekey))
,   b.clinicID
into ##MonthBase2
from dim.calendarv cv with (nolock)
	cross apply fct.enrtablev b with (nolock)
where cv.datekey >= '6/1/2015' and cv.datekey <= getdate()
"""

TempTREX4 = """
----------------All the months where a hospital is TREX-------------------
if object_id('tempdb..##TREXinMonth', 'u') IS NOT NULL
	drop table ##TREXinMonth
Select mb.datekey
,	 mb.clinicid
,	 ISNULL(IIF(trxh.KeyMo >= trxh.TREXStartMo, 1, 0), 0) as TREXinMonth
into ##TREXinMonth
from ##MonthBase2 mb
left join ##trxhosps trxh with (nolock) on (mb.datekey = trxh.KeyMo AND mb.clinicid = trxh.ClinicId)
group by mb.datekey
,	 mb.clinicid
,	 ISNULL(IIF(trxh.KeyMo >= trxh.TREXStartMo, 1, 0), 0)
--order by mb.clinicid, mb.datekey
"""

TempTREX5 = """
-----------Combining Enrollments with TREX in Month Flag--------------
if object_id('tempdb..##FullList2', 'u') IS NOT NULL
	drop table ##FullList2
select TM.datekey
,	TM.clinicid
,	ISNULL(E.Hosps, 0) AS Hosps
,	ISNULL(E.Pets, 0) AS Pets
,	ISNULL(TM.TREXinMonth, 0) AS TREXinMonth
into ##FullList2
from ##TREXinMonth TM
full outer join ##Enrs2 E with (nolock) on (TM.clinicid = E.clinicid AND TM.datekey = E.EnrMo)
group by TM.datekey
,	TM.ClinicId
,	ISNULL(E.Hosps, 0)
,	ISNULL(E.Pets, 0)
,	ISNULL(TM.TREXinMonth, 0)
--order by TM.clinicid, TM.datekey
"""

TempTREX6 = """
-----------------Active and TREX by Month--------------------  
if object_id('tempdb..##ClinicMonth2','u') is not null
    drop table ##ClinicMonth2
select
    ##FullList2.datekey
,   ##FullList2.clinicid
,	cp.PartnerId
,	IIF(e1.TREXinMonth + e2.TREXinMonth + e3.TREXinMonth > 0, 1, 0) AS TREXFlag
,   isnull(e1.pets,0) + isnull(e2.pets,0) + isnull(e3.pets,0) AS Pets90
into ##ClinicMonth2
from ##FullList2
	left join dw.ClinicPartner cp with (nolock) on (##FullList2.clinicid = cp.ClinicId) AND cp.EndDate IS NULL
    left join ##FullList2 e1 on e1.datekey = ##FullList2.datekey and e1.clinicID = ##FullList2.clinicID
    left join ##FullList2 e2 on e2.datekey = dateadd(m,-1,##FullList2.datekey) and e2.clinicID = ##FullList2.clinicID
    left join ##FullList2 e3 on e3.datekey = dateadd(m,-2,##FullList2.datekey) and e3.clinicID = ##FullList2.clinicID
order by ##FullList2.clinicid, ##FullList2.datekey
"""

TempTREX7 = """
----------------- FINAL SELECT STATEMENT ------------------
if object_id('tempdb..##TREXFinal','u') is not null
    drop table ##TREXFinal
select
    cm.datekey
,	cm.PartnerId
,   sum(iif(cm.Pets90 >= 1,1,0)) AS ActiveHosps
,	sum(iif(cm.Pets90 >= 1, cm.TREXFlag, 0)) AS trxhosps
into ##TREXFinal
from ##ClinicMonth2 cm
group by cm.datekey
,	cm.PartnerId
order by cm.PartnerId, cm.datekey
"""

TempDTC1 = """
if object_id('tempdb..##DTCFinal','u') is not null
    drop table ##DTCFinal
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
into ##DTCFinal
from fct.EnrTableV b with (nolock)
left join dw.PartnerTraitV ptv with (nolock) on (b.partnerid = ptv.partnerid)
left join sse.DTCCampaigns dtc1 with (nolock) on (b.PostalCode = dtc1.zipcode)
left join sse.DTCCampaigns dtc2 with (nolock) on (LEFT(b.PostalCode, 3) = dtc2.FSA)
where b.EnrollDate >= '10/1/2015'
group by DATEFROMPARTS(year(b.enrolldate), month(b.enrolldate), 1)
, IIF(ptv.ActiveTruDat = 0, 998, ISNULL(b.partnerid, 999))
"""

query = """
------------FINAL SELECT------------
SELECT MONTH(b.EnrollDate) AS 'EnrMo'
, YEAR(b.EnrollDate)-2014 AS 'EnrYr'
, cd.TrueCreditPartner AS 'PartnerId'
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
LEFT JOIN ##TPPFinal TPP on (DATEFROMPARTS(YEAR(b.enrolldate), MONTH(b.enrolldate), 1) = TPP.datekey AND cd.TrueCreditPartner = TPP.PartnerId)
LEFT JOIN ##TREXFinal TREX on (DATEFROMPARTS(YEAR(b.enrolldate), MONTH(b.enrolldate), 1) = TREX.datekey AND cd.TrueCreditPartner = TREX.PartnerId)
LEFT JOIN ##DTCFinal DTC  on (DATEFROMPARTS(YEAR(b.enrolldate), MONTH(b.enrolldate), 1) = DTC.EnrMo AND cd.TrueCreditPartner = DTC.PartnerId)
WHERE b.EnrollDate >= '10/1/2015'
AND b.EnrollDate < '10/1/2018'
AND TPP.TPPHosps IS NOT NULL
GROUP BY MONTH(b.EnrollDate)
, YEAR(b.EnrollDate)-2014
, cd.TrueCreditPartner
, TPP.TPPHosps
, TREX.trxhosps
, TPP.ActiveHosps
, IIF(TPP.ActiveHosps = 0, 0, CAST(TPP.TPPHosps AS DECIMAL(5))/CAST(TPP.ActiveHosps AS DECIMAL(5)))
, IIF(TREX.ActiveHosps = 0, 0, CAST(TREX.trxhosps AS DECIMAL(5))/CAST(TREX.ActiveHosps AS DECIMAL(5)))
, ISNULL(DTC.RadioFlag, 0)
, ISNULL(DTC.TVFlag, 0)
ORDER BY cd.TrueCreditPartner
, YEAR(b.EnrollDate)-2014
, MONTH(b.EnrollDate)
"""


# Connect to and query data warehouse
engine = create_engine('mssql+pyodbc://sav-dwh1')

connection = engine.connect()
connection.execute(TempTPP1)
connection.execute(TempTPP2)
connection.execute(TempTPP3)
connection.execute(TempTPP4)
connection.execute(TempTPP5)
connection.execute(TempTPP6)
connection.execute(TempTPP7)
connection.execute(TempTREX1)
connection.execute(TempTREX2)
connection.execute(TempTREX3)
connection.execute(TempTREX4)
connection.execute(TempTREX5)
connection.execute(TempTREX6)
connection.execute(TempTREX7)
connection.execute(TempDTC1)
df = pd.read_sql(query,connection)
connection.close()


# In[28]:


df.head()


# Inspect dataframe

# In[29]:


df.shape


# In[30]:


df.dtypes


# In[46]:


## Create a training set and a testing set. Relative sizes to change later maybe.

train = df.sample(frac=0.75, random_state=99)
test = df.loc[~df.index.isin(train.index), :]


# In[47]:


## Break off the explanatory variables from the independent variable.

x_train = train[['PartnerId', 'EnrMo', 'EnrYr', 'ActiveHosps', 'TPPRatio', 'TrexRatio', 'RadioFlag', 'TVFlag']]
y_train = train['Enrolls']
x_test = test[['PartnerId', 'EnrMo', 'EnrYr', 'ActiveHosps', 'TPPRatio', 'TrexRatio', 'RadioFlag', 'TVFlag']]
y_test = test['Enrolls']

x_train.head(3)


# In[48]:


train.shape


# In[49]:


# Checking for nulls here, forced to the average of a policyholder if NaN in query

np.any(np.isnan(train['TVFlag']))


# In[50]:


# Checking for nonfinite values here, similar to above

np.all(np.isfinite(train['TVFlag']))


# Since we have categorical variables - PartnerId and EnrMo - that haven't been converted to binary yet, we need to encode these as n-1 dummy variables in order to put them in our model and avoid perfect multicollinearity. Despite being ints, they aren't ordinal.

# In[51]:


# Everything will be relative to January

df_enrMo = pd.get_dummies(x_train['EnrMo'], drop_first = True)
df_enrMo = df_enrMo.rename(index = str, columns={2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun", 7:"Jul",
                                                8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"})


# In[52]:


# Everything will be relative to Drew Bowles (PartnerId = 4)

df_partners = pd.get_dummies(x_train['PartnerId'], drop_first = True)
df_partners = df_partners.rename(index = str, columns={12:"Skedden", 13:"RawlingsSr", 15:"Belanger", 20:"Bates", 25:"Pollock",
                                                       30:"Ferraro", 36:"Flaherty", 40:"Segura", 42:"Ladd", 48:"Rosen",
                                                       74:"Markham", 78:"DeRoy", 81:"LewMartin", 94:"Orloff", 113:"CarneyJr",
                                                       123:"Offer", 127:"Thue", 143:"Sterbenz", 154:"Trader", 160:"Rosen2",
                                                       171:"Howard", 193:"Moreau", 195:"Bailes", 199:"Earley", 205:"Hahn",
                                                       212:"Thome", 218:"Bacon", 224:"Kernohan", 230:"Nahrwold", 237:"Maddox",
                                                       238:"Thims", 240:"Flessatti", 241:"Belanger2", 246:"Schwarz", 
                                                       248:"RawlingsJr", 253:"Schneider", 259:"Ladd2", 260:"WestchesterCJ",
                                                       261:"Fischer", 265:"Klassen", 266:"Roach", 272:"NChicago", 
                                                       273:"SChicago", 281:"Cobb", 282:"Manwell", 284:"Rybicka", 
                                                       285:"CarneyJr2", 289:"Earley2", 291:"Pikula", 292:"Campbell",
                                                       296:"TampaEB", 297:"NewYork", 298:"NewJersey", 299:"Ellner",
                                                       301:"Flessatti2", 302:"Schneider2", 303:"Restuccia", 309:"AtlantaW",
                                                       310:"AtlantaE", 311:"Halsall", 312:"Proksel", 313:"IslandDR",
                                                       314:"JerseyCF", 317:"Medearis", 318:"Warner", 319:"Klassen2",
                                                       321:"AnHunter", 322:"Henry", 323:"McNicol", 324:"TucsonKB",
                                                       325:"Doran", 326:"Dempsey"})


# In[53]:


# Index type must be converted to int for the following concatenation.

df_enrMo = df_enrMo.astype(int)
df_enrMo.index = df_enrMo.index.astype(int)
df_partners = df_partners.astype(int)
df_partners.index = df_partners.index.astype(int)


# In[54]:


# Concatenate the booleans, drop the int versions, and add a contant.

new_x = pd.concat([x_train, df_enrMo, df_partners], axis=1)
new_x = new_x.drop(['PartnerId', 'EnrMo'], axis = 1)
new_x = smt.add_constant(new_x, prepend = True, has_constant = 'raise')
new_x.head()


# In[55]:


# Let's actually look at the distribution of monthly Enrollments by TP
get_ipython().run_line_magic('matplotlib', 'inline')

y = train['Enrolls']

sns.distplot(y)
plt.show()


# #### First Attempt - Poisson Regression
# We utilize a Poisson regression here because our independent variable, Enrolls, is a count with a relatively small range. Since the distribution of the error terms will therefore not be independent and identically distributed we do not use OLS.

# In[56]:


poisson = sm.GLM(y_train, new_x, family = Poisson()).fit()
# poisson.summary()


# In[57]:


y_train.mean()


# In[58]:


y_train.var()


# In[64]:


alpha = (y_train.var()-y_train.mean())/(y_train.mean()*y_train.mean())
alpha


# #### Second Attempt - Negative Binomial
# Shouldn't use Poisson, because the variance does not equal the mean. Trying a Negative Binomial instead.

# In[62]:


negbinomial = sm.GLM(y_train, new_x, family = sm.families.NegativeBinomial(alpha = alpha)).fit()
negbinomial.summary()


# In[85]:


## This cell prints the summary to a version that can be easily copied into our existing Excel document for wider audience.
## Only run if necessary.

#coef_df = negbinomial.summary().as_csv()
#coef_df.split('\n')


# In[64]:


negbinomial.get_prediction()


# In[22]:


dir(negbinomial)


# In[23]:


sns.pairplot(train, vars=["EnrYr", "TPPHospitals", "TrexHospitals", "ActiveHosps", "TPPRatio", "TrexRatio", "Enrolls"])


# In[21]:


sns.pairplot(train, size=4, hue = "TVFlag", 
             vars=["TPPHospitals", "TrexHospitals", "ActiveHosps", "TPPRatio", "TrexRatio", "Enrolls"])


# In[22]:


sns.pairplot(train, size=4, hue = "RadioFlag", 
            vars=["TPPHospitals", "TrexHospitals", "ActiveHosps", "TPPRatio", "TrexRatio", "Enrolls"])


# ## Links I've saved
# 
# https://www.statsmodels.org/dev/glm.html -- GLM Page of all these equations, Poisson, Negative Binomial, etc.
# http://www.karlin.mff.cuni.cz/~pesta/NMFM404/NB.html  -- Intuition behind negative binomial regression.
# 
