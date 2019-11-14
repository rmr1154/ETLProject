# ETLProject

Beer Reviews and Breweries
- https://www.kaggle.com/brkurzawa/us-breweries
- https://www.kaggle.com/rdoume/beerreviews


Extract</br>
<hr>
Beer Reviews
- download kaggle dataset and explode zip, break up csv into <25MB pieces for github
- load csv's into Pandas and reassemble csv parts
</br>
US Breweries
- load csv's into Pandas and clean

Transform</br>
<hr>
Beer Reviews
- download kaggle dataset and explode zip, break up csv into <25MB pieces for github
- load csv's into Pandas and reassemble csv parts
- transform string to datetime
- transform strings to floats
</br>
US Breweries
- join to Beer Reviews where data exists
- transform addresses into city, state, zip
</br>
Load</br>
<hr>
- load data sets into SQLite database
