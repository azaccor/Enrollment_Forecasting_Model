# Enrollment_Forecasting_Model
This code is designed to take the query in the first part of the code and then predict gross new enrollments by month and year for each territory partner. We utilize a negative binomial model because the enrollments we are predicting are counts and the variance is about
100 times larger than the mean. 

Edits:
We ended up pulling most of the variables out of this model and stripped it down to just partner, month, and year. Since predicting Trupanion Express/Partners Program hospital ratios, or number of active hospitals, would require models and assumptions of their own, it doesn't really make sense to include these as predictor variables. 
I admit, I should have protested this right when we got started, but my supervisor asked me to build a model that would allow us to tweak these inputs and see the expected effect of those changes on the enrollments for that month and I happily complied since it reprieved me from Excel for a minute. 
I think I'm going to go back to the drawing board with this. I tested the data set in DataRobot, and it suggested using extreme gradient boosting on just partner, month, and year. I was surprised to see that the seasonality components recieved such little weight in the model, but the results spoke for themselves. Just using those three simple variables, it was able to predict the enrollments in the training set with an RMSE of about 23. The MAE was around 13 so not really that bad.
