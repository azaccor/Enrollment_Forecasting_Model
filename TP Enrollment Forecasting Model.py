
# coding: utf-8

# ## Predicting Conversions from Web Quotes

# In[1]:


import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn import linear_model
from sklearn import feature_selection
import statsmodels.api as sm
import pyodbc
import random


# Import the data from SQL Server

# In[2]:


query = """
/* Data Set for Enrollment Forecasting Model */
select 
  CASE
	WHEN ptv.ActiveTruDat = 0 THEN 998   --Termed TP
	ELSE ISNULL(b.partnerid, 999) END AS 'PartnerId'
, MONTH(b.EnrollDate) as EnrMo
, YEAR(b.enrolldate)-2015 as EnrYr
, IIF(b.CountryId = 2, 1, 0) as 'CanFlag'
, IIF(b.State = 'CA', 1, 0) as 'CAFlag'
, CASE 
	WHEN b.CountryId = 1 THEN ISNULL(dz.incomePerHousehold, 60000)
	WHEN b.CountryId = 2 THEN ISNULL(fsa.FSAmedianincome, 70000)
	END AS 'MedianIncome'
, CASE 
	WHEN b.CountryId = 1 THEN ISNULL(dz.PopDensity, 1200)
	WHEN b.CountryId = 2 THEN ISNULL(cad.PopDensityMi, 888)
	END AS 'PopDensity'
, CASE
	WHEN ct.SignUpImplementationDate <= b.EnrollDate AND ct.TPPFlag = 1 THEN 1
	ELSE 0 END AS 'TPPFlag'
, CASE
	WHEN ct.TREXLiveDate <= b.EnrollDate AND ct.TREXFlag = 1 THEN 1
	ELSE 0 END AS 'TREXFlag'
, ISNULL(dtc.DTCFlag, 0) AS 'DTCFlag'
, COUNT(DISTINCT b.ppid) AS 'Enrolls'

FROM fct.enrtableV b with (nolock)
LEFT join dw.State s with (nolock) on (b.State = s.StateCode)
LEFT join sse.DemographicsByZip dz with (nolock) on (b.PostalCode = dz.Zipcode)
LEFT join sse.CanadianDemographics cad with (nolock) on (LEFT(b.PostalCode, 3) = cad.FSA)
LEFT join sse.GLMCanadaFSAIncome fsa with (nolock) on (LEFT(b.PostalCode, 3) = fsa.FSA)
LEFT join sse.clinictrait ct with (nolock) on (b.clinicid = ct.clinicid)
LEFT join (
		SELECT b.ppid
		, b.PostalCode
		, b.EnrollDate
		, ISNULL(dtc1.StartDate, dtc2.StartDate) AS 'StartDate'
		, ISNULL(dtc1.EndDate, dtc2.EndDate) AS 'EndDate'
		, 1 AS 'DTCFlag'
		FROM fct.enrtableV b with (nolock)
		LEFT join sse.DTCCampaigns dtc1 with(nolock) on (b.PostalCode = dtc1.Zipcode) --Join in the American DTC Tests
		LEFT join sse.DTCCampaigns dtc2 with (nolock) on (LEFT(b.PostalCode, 3) = dtc2.FSA) --Join in the Canadian DTC Tests
		WHERE b.EnrollDate >= ISNULL(dtc1.StartDate, dtc2.StartDate)
		AND b.EnrollDate <= ISNULL(DATEADD(DAY, 14, dtc1.EndDate), DATEADD(DAY, 14, dtc2.EndDate)) --Includes some DTC runout
		AND b.EnrollDate >= '1/1/2016'
	) dtc on (b.ppid = dtc.ppid)
LEFT join dw.PartnerTraitV ptv with (nolock) on (b.partnerid = ptv.PartnerId)

WHERE b.EnrollDate >= '1/1/2016'
AND b.SameDayCancel = 0
AND b.NotPaidByCorpOrTruEmp = 0

GROUP BY
  CASE
	WHEN ptv.ActiveTruDat = 0 THEN 998
	ELSE ISNULL(b.partnerid, 999) END
, MONTH(b.EnrollDate)
, YEAR(b.enrolldate)-2015
, IIF(b.CountryId = 2, 1, 0)
, IIF(b.State = 'CA', 1, 0)
, CASE 
	WHEN b.CountryId = 1 THEN ISNULL(dz.incomePerHousehold, 60000)
	WHEN b.CountryId = 2 THEN ISNULL(fsa.FSAmedianincome, 70000)
END
, CASE 
	WHEN b.CountryId = 1 THEN ISNULL(dz.PopDensity, 1200)
	WHEN b.CountryId = 2 THEN ISNULL(cad.PopDensityMi, 888)
END
, CASE
WHEN ct.SignUpImplementationDate <= b.EnrollDate AND ct.TPPFlag = 1 THEN 1
ELSE 0 END
, CASE
WHEN ct.TREXLiveDate <= b.EnrollDate AND ct.TREXFlag = 1 THEN 1
ELSE 0 END
, ISNULL(dtc.DTCFlag, 0)


"""

# Call ODBC connection to Data Warehouse
engine = create_engine('mssql+pyodbc://sav-dwh1')
connection = engine.connect()

# Read query results into a pandas dataframe
df = pd.read_sql(query,connection)

connection.close()


# Inspect dataframe

# In[3]:


df.head()


# In[4]:


df.shape


# In[5]:


df.dtypes


# In[6]:


## Create a training set and a testing set. Relative sizes to change later maybe.

train = df.sample(frac=0.8, random_state=123)
test = df.loc[~df.index.isin(train.index), :]


# In[7]:


## Break off the explanatory variables

x_train = train[['PartnerId', 'EnrMo', 'EnrYr', 'CanFlag', 'CAFlag', 'MedianIncome', 'PopDensity',
                'TPPFlag', 'TREXFlag', 'DTCFlag']]
y_train = train['Enrolls']
x_test = test[['PartnerId', 'EnrMo', 'EnrYr', 'CanFlag', 'CAFlag', 'MedianIncome', 'PopDensity',
                'TPPFlag', 'TREXFlag', 'DTCFlag']]
y_test = test['Enrolls']

x_train.head(3)


# In[8]:


x_test.shape


# In[9]:


# Checking for nulls here, forced to the average of a policyholder if NaN in query

np.any(np.isnan(x_train['PopDensity']))


# In[10]:


# Checking for nonfinite values here, similar to above

np.all(np.isfinite(x_train['PopDensity']))


# Since we have categorical variables - PartnerId and EnrMo - that haven't been converted to binary yet, we need to encode these as n-1 dummy variables in order to put them in our model and avoid perfect multicollinearity. Despite being ints, they aren't ordinal.

# In[11]:


# Everything will be relative to January

df_enrMo = pd.get_dummies(x_train['EnrMo'], drop_first = True)
df_enrMo = df_enrMo.rename(index = str, columns={2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun", 7:"Jul",
                                                8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"})


# In[12]:


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
                                                       321:"AnHunter", 322:"Henry", 323:"McNicol", 324:"TucsonKB", 325:"Doran",
                                                       998:"Termed", 999:"NoPartner"})


# In[13]:


# Index type must be converted to int for the following concatenation.

df_enrMo = df_enrMo.astype(int)
df_enrMo.index = df_enrMo.index.astype(int)
df_partners = df_partners.astype(int)
df_partners.index = df_partners.index.astype(int)


# In[14]:


new_x = pd.concat([x_train, df_enrMo, df_partners], axis=1)
new_x = new_x.drop(['PartnerId', 'EnrMo'], axis = 1)
new_x.head()


# #### First Attempt - Multiple Linear Regression

# In[15]:


lm = linear_model.LinearRegression()
lm.fit(new_x, y_train)


# In[17]:


lm = sm.OLS(y_train, new_x).fit()


# In[18]:


lm.summary()


# #### Second Attempt - No Demographic Variables
# I want to force the model to utilize the TPs as variables as much as possible without them being absorbed by the zipcode or state level data.

# In[19]:


## Break off the explanatory variables

x_train2 = train[['PartnerId', 'EnrMo', 'EnrYr', 'TPPFlag', 'TREXFlag', 'DTCFlag']]
y_train2 = train['Enrolls']
x_test2 = test[['PartnerId', 'EnrMo', 'EnrYr', 'TPPFlag', 'TREXFlag', 'DTCFlag']]
y_test2 = test['Enrolls']

x_train2.head(3)


# In[20]:


# Everything will be relative to January

df_enrMo = pd.get_dummies(x_train2['EnrMo'], drop_first = True)
df_enrMo = df_enrMo.rename(index = str, columns={2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun", 7:"Jul",
                                                8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"})


# In[21]:


# Everything will be relative to Drew Bowles (PartnerId = 4)

df_partners = pd.get_dummies(x_train2['PartnerId'], drop_first = True)
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
                                                       321:"AnHunter", 322:"Henry", 323:"McNicol", 324:"TucsonKB", 325:"Doran",
                                                       998:"Termed", 999:"NoPartner"})


# In[26]:


# Index type must be converted to int for the concatenation.

df_enrMo = df_enrMo.astype(int)
df_enrMo.index = df_enrMo.index.astype(int)
df_partners = df_partners.astype(int)
df_partners.index = df_partners.index.astype(int)


# In[28]:


new_x = pd.concat([x_train2, df_enrMo, df_partners], axis=1)
new_x = new_x.drop(['PartnerId', 'EnrMo'], axis = 1)
new_x = sm.add_constant(new_x)
new_x.head()


# In[29]:


lm = sm.OLS(y_train2, new_x).fit()
lm.summary()


# In[30]:


y_train.groupby(x_train.EnrMo).mean()

