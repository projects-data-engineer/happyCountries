# happyCountries


1. [Create env and install pyspark and jupyter lab](#schema1)
2. [Dataset](#schema2)
3. [ Data loading and cleaning](#schema3)
4. [Exploratory Data Analysis (EDA)](#schema4)
5. [Clustering with MLlib in PySpark (K-means)](#schema5)
6. [Conclusion](#schema6)
7. [Resources](#schemaref)

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

## 3. Data loading and cleaning

In this section:
- We load the dataset, 
- join this dataset to add the year of each dataset.
- We calculate the basic statistics.
- Analyze the null values, clean data with nulls


<hr>
<a name='schema4'></a>

## 4. Exploratory Data Analysis (EDA)

In this section:
- we calculate the distribution of Happiness, the correlation of factors with happiness, to find the correlation between each factor and the Happiness_Score, we will use the correlation function in PySpark for each factor. Here we assume that there are several columns of factors, e.g. GDP_Per_Capita, Social_Support, Life_Expectancy, etc.

```
+--------------------+-------------------+
|              Factor|        Correlation|
+--------------------+-------------------+
|      GDP_per_Capita|  0.723784949432059|
|Healthy_Life_Expe...|  0.682479558984849|
|      Social_Support| 0.6498436290451899|
|Freedom_to_Make_L...| 0.5709017429407508|
|Perceptions_of_Co...| 0.4149541173536084|
|          Generosity|0.08210993118693721|
+--------------------+-------------------+
```
Conclusion: The happiness is correlation with GDP_Per_Capita, Health_life_expectancy, social_support in this orden

- We analyze annual and regional trends

```
+----+-------------------+
|year|AVG_Happiness_Score|
+----+-------------------+
|2022|   5.55357534246575|
|2023|  5.544441176470589|
|2021|  5.532838926174494|
|2020|   5.47323986284967|
|2019|  5.407096153846153|
|2016|  5.382184713375795|
|2015| 5.3757341772151905|
|2018|  5.366896774193549|
|2017|  5.354019355774192|
+----+-------------------+
```
Conclusion: 2022 year mores happiness

```
+----------------------------------+-------------------+
|Region                            |AVG_Happiness_Score|
+----------------------------------+-------------------+
|North America and ANZ             |7.176058332472223  |
|Western Europe                    |6.811460638909575  |
|Latin America and Caribbean       |5.993860526805262  |
|East Asia                         |5.732149987592593  |
|Commonwealth of Independent States|5.639040905886364  |
|Central and Eastern Europe        |5.585753270855141  |
|Southeast Asia                    |5.3704287529       |
|Middle East and North Africa      |5.26798553601258   |
|Africa                            |5.057              |
|South Asia                        |4.473263924868853  |
|Sub-Saharan Africa                |4.288796142305638  |
+----------------------------------+-------------------+
```
Conclusion: North America and ANZ  region more happiness

- Regional grouping and calculation of average_happiness_score and other factors

```
+--------------------+-------------------+-------------------+------------------+---------------------------+-------------------+-------------------+-----------------------------+
|              Region|AVG_Happiness_Score| AVG_GDP_per_Capita|AVG_Social_Support|AVG_Healthy_Life_Expectancy|        AVG_Freedom|     AVG_Generosity|AVG_Perceptions_of_Corruption|
+--------------------+-------------------+-------------------+------------------+---------------------------+-------------------+-------------------+-----------------------------+
|North America and...|  7.176058332472223| 1.5018248203888886|1.3518091635555554|         0.8178462977222222| 0.6059892894444444|0.33619929672222226|           0.3067956118888889|
|      Western Europe|  6.811460638909575| 1.4904419756648937|1.2974117135797874|         0.8435619392446807| 0.5480937489414893|0.23022084629787232|          0.24835307204787233|
|Latin America and...|  5.993860526805262| 1.0168062146210526|1.1473719096210526|         0.6477685120105263| 0.5048632066578947| 0.1624974665842105|          0.09478198188421051|
|           East Asia|  5.732149987592593| 1.3502869296481481|1.1684111601296296|         0.8048310162037038| 0.4437673237777778|0.17068215757407407|          0.14275146933333333|
|Commonwealth of I...|  5.639040905886364| 0.8921698679545454|1.2586758695909088|         0.5680926035681819| 0.5420652966136363|0.24484552461363637|           0.1451973231590909|
|Central and Easte...|  5.585753270855141| 1.1897778031822428|1.1512417811495326|         0.6771699339813085|0.40395033210280373|0.13218065540186916|          0.07025546177570093|
|      Southeast Asia|       5.3704287529|    1.0158165927375|   1.0568352877125|            0.5935442974625|      0.58934856345|       0.3325270834|          0.14416829373749998|
|Middle East and N...|   5.26798553601258|  1.157150908144654|0.9732086901006292|         0.6319705412955975| 0.3733136363962264|0.15596198787421384|           0.1356116103962264|
|              Africa|              5.057|0.22202499999999997|           0.85507|                   0.384905|            0.42856|           0.508985|                      0.38361|
|          South Asia|  4.473263924868853| 0.7328129685409837|0.7473223635081967|         0.4896550032622951| 0.4237290859180328| 0.2534539122459016|          0.09941797308196722|
|  Sub-Saharan Africa|  4.288796142305638| 0.5509399656706231|0.7860001713649851|         0.2790738291275964|0.37760817261127594|0.19472393034718097|          0.10628202214836797|
+--------------------+-------------------+-------------------+------------------+---------------------------+-------------------+-------------------+-----------------------------+
```
North America and ANZ  region more happiness

- Grouping by Country and calculation of average Happiness_Score and other factors 
```
+--------------+-------------------+------------------+------------------+---------------------------+-------------------+-------------------+-----------------------------+
|       Country|AVG_Happiness_Score|AVG_GDP_per_Capita|AVG_Social_Support|AVG_Healthy_Life_Expectancy|        AVG_Freedom|     AVG_Generosity|AVG_Perceptions_of_Corruption|
+--------------+-------------------+------------------+------------------+---------------------------+-------------------+-------------------+-----------------------------+
|       Finland|  7.662744438666666|1.4773323815555555| 1.402296958888889|         0.8201610083333333| 0.6632219552222223|0.17862035722222216|           0.4466920894444445|
|       Denmark|  7.579733297555555|1.5238435041111111|1.4045434216666668|         0.8202864545555556|  0.659315179888889|0.27124154277777784|           0.4649033773333333|
|       Iceland|  7.522277788777779|1.5114571808888888|1.4581812771111113|         0.8581739475555555| 0.6562392647777777| 0.3634389387777778|          0.15480748122222224|
|   Switzerland|  7.493322196111112|1.5961770892222225|1.3696739057777778|         0.8780138304444445| 0.6377961163333334| 0.2398016702222222|           0.4061959095555555|
|        Norway|  7.473888899444445|1.6172345015555551|1.3909495623333337|         0.8318598224444445|        0.671493719|0.29010414700000003|          0.39087271144444447|
|   Netherlands|  7.417100008666667|       1.531334553|1.3316527953333333|         0.8281690521111111| 0.6146534408888888| 0.3595630443333333|           0.3398490652222222|
|        Sweden| 7.3434999787777775|1.5179047217777777|1.3391355412222223|         0.8454917354444444|  0.655689088888889| 0.2935985716666667|          0.43439723333333335|
|   New Zealand|  7.273844472666667|1.4359848847777779|1.3954292782222222|                0.835136445| 0.6391891153333333|0.36008348188888895|          0.42892721977777776|
|        Canada|        7.230455555|1.4951435643333335|1.3431612104444446|         0.8496988441111112|  0.629088782111111|0.32339096022222225|          0.32754815244444446|
|     Australia|  7.227088855333333| 1.504091263111111|1.3664642466666668|         0.8555838307777779| 0.6274549527777779| 0.3551514292222222|           0.3196511276666667|
|        Israel|   7.20373332688889|1.4188973453333333|1.3143163042222221|         0.8560552161111111|0.46157756455555554|        0.261504939|          0.12491894999999999|
|       Austria| 7.1702444428888885|1.5176658667777778|1.3184733208888886|         0.8344813274444445| 0.5978905631111111| 0.2573902317777777|           0.2537285004444445|
|    Luxembourg|  7.097055567444445|1.7650055163333331|1.2909702316666665|         0.8447657977777777| 0.6198105484444445|0.20668721133333331|          0.35701621033333336|
|   Puerto Rico|              7.039|           1.35943|           1.08113|                    0.77758|            0.46823|            0.22202|                      0.12275|
|    Costa Rica|   7.00137776388889|1.1627093327777775|1.2567396957777777|         0.7855416223333334| 0.6242399325555554| 0.1562643933333333|          0.10030763122222221|
|       Ireland|  6.994744463222222|1.6304403796666667| 1.375430838666667|         0.8268148132222222| 0.5930822534444444|0.31676947977777775|           0.3395990461111111|
|       Germany|  6.977977795111111|1.5126553255555555|1.2942415661111109|         0.8219772814444444| 0.5733572795555557|0.24666298599999997|          0.29600380333333337|
| United States|  6.972844446888889|1.5720795693333334|1.3021819188888888|         0.7309660709999999| 0.5282243075555556|0.30617131555555555|          0.15105594766666666|
|United Kingdom|  6.946388940222222|1.4564327752222224|1.3187227836666666|         0.8224862243333333| 0.5385520770000001|0.37754306666666665|          0.27974330133333336|
|       Belgium|  6.885388879111111|1.4944259475555555|1.3043489206666665|         0.8242503353333333| 0.5294017936666666|        0.162818833|          0.22910524255555556|
+--------------+-------------------+------------------+------------------+---------------------------+-------------------+-------------------+-----------------------------+
```

Conclusion: Finland is country  more happiness

- Grouping by Country and Region and calculation of average Happiness_Score and other factors 

```
+----------------------------+--------------+-------------------+------------------+------------------+---------------------------+-------------------+-------------------+-----------------------------+
|Region                      |Country       |AVG_Happiness_Score|AVG_GDP_per_Capita|AVG_Social_Support|AVG_Healthy_Life_Expectancy|AVG_Freedom        |AVG_Generosity     |AVG_Perceptions_of_Corruption|
+----------------------------+--------------+-------------------+------------------+------------------+---------------------------+-------------------+-------------------+-----------------------------+
|Western Europe              |Finland       |7.662744438666666  |1.4773323815555555|1.402296958888889 |0.8201610083333333         |0.6632219552222223 |0.17862035722222216|0.4466920894444445           |
|Western Europe              |Denmark       |7.579733297555555  |1.5238435041111111|1.4045434216666668|0.8202864545555556         |0.659315179888889  |0.27124154277777784|0.4649033773333333           |
|Western Europe              |Iceland       |7.522277788777779  |1.5114571808888888|1.4581812771111113|0.8581739475555555         |0.6562392647777777 |0.3634389387777778 |0.15480748122222224          |
|Western Europe              |Switzerland   |7.493322196111112  |1.5961770892222225|1.3696739057777778|0.8780138304444445         |0.6377961163333334 |0.2398016702222222 |0.4061959095555555           |
|Western Europe              |Norway        |7.473888899444445  |1.6172345015555551|1.3909495623333337|0.8318598224444445         |0.671493719        |0.29010414700000003|0.39087271144444447          |
|Western Europe              |Netherlands   |7.417100008666667  |1.531334553       |1.3316527953333333|0.8281690521111111         |0.6146534408888888 |0.3595630443333333 |0.3398490652222222           |
|Western Europe              |Sweden        |7.3434999787777775 |1.5179047217777777|1.3391355412222223|0.8454917354444444         |0.655689088888889  |0.2935985716666667 |0.43439723333333335          |
|North America and ANZ       |New Zealand   |7.273844472666667  |1.4359848847777779|1.3954292782222222|0.835136445                |0.6391891153333333 |0.36008348188888895|0.42892721977777776          |
|North America and ANZ       |Canada        |7.230455555        |1.4951435643333335|1.3431612104444446|0.8496988441111112         |0.629088782111111  |0.32339096022222225|0.32754815244444446          |
|North America and ANZ       |Australia     |7.227088855333333  |1.504091263111111 |1.3664642466666668|0.8555838307777779         |0.6274549527777779 |0.3551514292222222 |0.3196511276666667           |
|Middle East and North Africa|Israel        |7.20373332688889   |1.4188973453333333|1.3143163042222221|0.8560552161111111         |0.46157756455555554|0.261504939        |0.12491894999999999          |
|Western Europe              |Austria       |7.1702444428888885 |1.5176658667777778|1.3184733208888886|0.8344813274444445         |0.5978905631111111 |0.2573902317777777 |0.2537285004444445           |
|Western Europe              |Luxembourg    |7.097055567444445  |1.7650055163333331|1.2909702316666665|0.8447657977777777         |0.6198105484444445 |0.20668721133333331|0.35701621033333336          |
|Latin America and Caribbean |Puerto Rico   |7.039              |1.35943           |1.08113           |0.77758                    |0.46823            |0.22202            |0.12275                      |
|Latin America and Caribbean |Costa Rica    |7.00137776388889   |1.1627093327777775|1.2567396957777777|0.7855416223333334         |0.6242399325555554 |0.1562643933333333 |0.10030763122222221          |
|Western Europe              |Ireland       |6.994744463222222  |1.6304403796666667|1.375430838666667 |0.8268148132222222         |0.5930822534444444 |0.31676947977777775|0.3395990461111111           |
|Western Europe              |Germany       |6.977977795111111  |1.5126553255555555|1.2942415661111109|0.8219772814444444         |0.5733572795555557 |0.24666298599999997|0.29600380333333337          |
|North America and ANZ       |United States |6.972844446888889  |1.5720795693333334|1.3021819188888888|0.7309660709999999         |0.5282243075555556 |0.30617131555555555|0.15105594766666666          |
|Western Europe              |United Kingdom|6.946388940222222  |1.4564327752222224|1.3187227836666666|0.8224862243333333         |0.5385520770000001 |0.37754306666666665|0.27974330133333336          |
|Western Europe              |Belgium       |6.885388879111111  |1.4944259475555555|1.3043489206666665|0.8242503353333333         |0.5294017936666666 |0.162818833        |0.22910524255555556          |
+----------------------------+--------------+-------------------+------------------+------------------+---------------------------+-------------------+-------------------+-----------------------------+
```



<hr>
<a name='schema5'></a>

## 5. Clustering with MLlib in PySpark (K-means)
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

<hr>
<a name='schema6'></a>

## 6. Conclusion
North America and ANZ and Western Europe are the happiest areas in this dataset, as they have the highest happiness scores.



<hr>
<a name='schemaref'></a>


## 7. Resources
https://www.kaggle.com/datasets/sazidthe1/global-happiness-scores-and-factors?select=WHR_2023.csv