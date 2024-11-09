# happyCountries


1. [Create env and install pyspark and jupyter lab](#schema1)
2. [Dataset](#schema2)
3. [Clustering with MLlib in PySpark (K-means)](#schema3)

[Resources](#schemaref)

<hr>
<a name='schema1'></a>

## 1. Create env and install pyspark and jupyter lab

1. Create env
```python
python3 -m venv pyspark-env
```
2. Activate
```python
source pyspark-env/bin/activate
```
3. Install Pyspark and jupyterlab
```python
pip install pyspark

pip install findspark

pip install jupyterlab
```
<hr>
<a name='schema2'></a>

## 2. Dataset

These datasets comprise various key indicators related to happiness, covering from 2015 up to 2023. It includes the following columns:


| Column                        | Description                                                        |
|-------------------------------|--------------------------------------------------------------------|
| country                       | The name of the country.                                           |
| region                        | The geographic region or continent.                                |
| happiness_score               | A measure reflecting overall happiness.                           |
| gdp_per_capita                | A measure of Gross Domestic Product per capita.                   |
| social_support                | A metric measuring social support.                                |
| healthy_life_expectancy       | A measure of years of healthy life expectancy.                    |
| freedom_to_make_life_choices  | A measure of freedom in life choices.                             |
| generosity                    | A metric reflecting generosity.                                   |
| perceptions_of_corruption     | A measure of perception of corruption within a country.           |

<hr>
<a name='schema3'></a>

## 3. Clustering with MLlib in PySpark (K-means)
### 1. Feature selection and standardization
**1. Select the columns:** The columns `GDP_per_capita`, `Social_support`, `Health_life_expectancy` and `Freedom` represent the economic and social characteristics that influence a country.

**2. VectorAssembler:** We create a VectorAssembler, which is a tool in PySpark to combine multiple columns into a single vector of features. VectorAssembler takes the selected columns and converts them into a single vector column called features. This is necessary since the K-means model in PySpark requires the features to be in vector format.

**3. Transform the DataFrame:** We use assembler.transform(df_cleaned) to apply the VectorAssembler to the DataFrame. This generates a new DataFrame (df_features) that includes the features column with the feature vectors.

### 2. Normalization:

**4. StandardScaler:** K-means is sensitive to the scale of the features, so it is common to normalize them so that they all have the same importance. `StandardScaler` normalizes each feature by dividing by its standard deviation.

**5. Fit the scaler:** We create a scaling model (scaler_model) and train it on the data with `scaler.fit(df_features).`

**6.Apply normalization:** We then use `scaler_model.transform(df_features)` to transform the features and generate a new column `scaled_features`. This DataFrame `(df_scaled)` now has the normalized features, ready for the K-means model.

### 3. K-means application

**7. Define the K-means model:** We specify `k=3` to create 3 clusters, which means that we will group the countries into three groups. Adjust k according to the number of clusters you want.

**8. Train the model:** `kmeans.fit(df_scaled)` trains the model on the scaled data, generating a trained model called model.

**9.Transform the DataFrame:** `model.transform(df_scaled)` predicts to which cluster each row of the DataFrame belongs and adds a cluster column indicating the group of each country.

### 4. Cluster interpretation

**10. Show the results:** We use `.select()` to select the Country, Region and cluster columns and display them with .show(). This allows you to see which cluster each country belongs to.

**11. Cluster interpretation:** Look at the patterns in the clusters. You can see if countries in the same cluster share similarities in factors such as GDP, social support or freedom, which may reveal common trends.




## Resources
https://www.kaggle.com/datasets/sazidthe1/global-happiness-scores-and-factors?select=WHR_2023.csv