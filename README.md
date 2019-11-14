# ETLProject

Beer Reviews and Breweries
- https://www.kaggle.com/brkurzawa/us-breweries
- https://www.kaggle.com/rdoume/beerreviews


Extract</br>
<hr>
Beer Reviews</br>
- download kaggle dataset and explode zip, break up csv into <25MB pieces for github</br>
- load csv's into Pandas and reassemble csv parts</br>
</br>
US Breweries
- load csv's into Pandas and clean</br>
</br>
</br>
Transform</br>
<hr>
Beer Reviews</br>
- download kaggle dataset and explode zip, break up csv into <25MB pieces for github</br>
- load csv's into Pandas and reassemble csv parts</br>
- transform string to datetime</br>
- transform strings to floats</br>
</br>
US Breweries</br>
- join to Beer Reviews where data exists</br>
- transform addresses into city, state, zip</br>
</br>
</br>
Load</br>
<hr>
- load data sets into SQLite database</br>
