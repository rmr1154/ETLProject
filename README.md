# ETLProject

Beer Reviews and Breweries
- https://www.kaggle.com/brkurzawa/us-breweries
- https://www.kaggle.com/rdoume/beerreviews

Transformations
Beer Reviews
- download kaggle dataset and explode zip, break up csv into <25MB pieces for github
- load csv's into Pandas and reassemble csv parts
- transform string to datetime
- transform strings to floats

US Breweries
- join to Beer Reviews where data exists
- transform addresses into city, state, zip

