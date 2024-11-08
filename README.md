# happyCountries


1. [Create env and install pyspark and jupyter lab](#schema1)
2. [Dataset](#schema2)

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






## Resources
https://www.kaggle.com/datasets/sazidthe1/global-happiness-scores-and-factors?select=WHR_2023.csv