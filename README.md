# lichess_ml_prediction
Python Metadata-Analysis of the Lichess Games Database to check machine learning approaches for the prediction of games outcome.

## Still under construction
Finished:

1. Obtain and process database
* Download database archive as stream, stream decompress png-file and extract game meta data on the fly

2. Feature engineering in spark
* Create player database
* Replace players by UserIDs 
* Data cleansing 
* Create new features (Elo difference, playing streak over 3H & 1M, winning streak, losing streak)

3. Data analytics 
* Visual inspection
* Distribution of player ratings
* Distribution of wins
* Popular openings
* Popular openings by player classes
* ongoing - a couple more topics are prepared, not publishable yet

4. Prediction via machine learning
* Simple Elo difference model
* Random Forest (untuned)
* Gradient Boosting (untuned)
* Future work will deal with
   * parameter tuning
   * dimension reduction with pipelines
   * cross validation
   * feature hashing for player columns (white, black) and opening (eco codes)
   * further modules (xgboost, pyspark.ml)
   * further algorithms
   * clustering, unlikely to work well
   * neuronal networks (interesting to test)

Current best score: AUC = 0.7
