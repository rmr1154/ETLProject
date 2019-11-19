# ETLProject

Instructions</br>
<hr>
- Run ETL_app.py</br>
</br>
</br>
ETL_app.py loads the following modules
</br>
<ul>
  <li>etl_tools.py - supporting toolset</li>
  <li>extract.py - main extract module</li>
  <li>transform.py - main transform module</li>
  <li>load.py - main load module</li>
  </ul>
  </br>
  <h5>Modules Used</h5>
  cleanco (pip install cleanco)</br>
  zipfile</br>
  pandas</br>
  sqlite3</br>
  numpy</br>
  time</br>
  


Beer Reviews and Breweries
- https://www.kaggle.com/brkurzawa/us-breweries
- https://www.kaggle.com/rdoume/beerreviews


Extract</br>
<hr>
- Download the datasets from kaggle and store them into zip files to make it lighter</br>
- Read the csv's with pandas in order to start cleaning the data</br>
</br>
</br>

Transform</br>
<hr>
Beer Reviews</br>
- Trasnform the Beer review_time column to date time format</br>
- Create brewery_names_df to speed up etl.clean_co_names() processing</br>
- Drop Duplicate brewery_name</br>
- transform strings to floats </br>
- Clean company names and make them uniform: set to upercase, remove commas, remove hyphens, remove text between parenthesis, replace AND with &, remove leading and trailing spaces, remove dots, make sure they are encode utf-8.</br>
- Drop Duplicate Names after etl.clean_co_names()</br
- Join brewery_names_df back in</br>
-Stripe whitespaces from all columns</br>
- Drop reviews for unknown brewries<br/>
- Set final indexes and column names</br>
</br>

US Breweries</br>
- join to Beer Reviews where data exists</br>
- clean the websites url's: removing http:// and htpps://, removing irrelevant information, , flag bad websites, flag bad location addresses.</br>
- Clean company names and make them uniform: set to upercase, remove commas, remove hyphens, remove text between parenthesis, replace AND with &, remove leading and trailing spaces, remove dots, make sure they are encode utf-8.</br>
- Breakdown the address into Zip code, city, street and state: Split location addresses, strip whitespace from all columns, reorder columns</br>
- Determine closed locations</br>
- Set final indexes and column names</br>

</br>
</br>
Load</br>
<hr>
- Build dict of datasets for export</br>
- LOAD to SQLite @ /resources/{dbname}.db</br>
- drop table if exists "breweries"</br>
- create and load table - breweries</br>
- drop table if exists "brewery_locations"</br>
- create and load table - brewery_locations</br>
- drop table if exists "beers"</br>
- create and load table - beers</br>
- drop table if exists "reviews"</br>
- create and load table - reviews</br>

</br>
</br>
Overview</br>
<hr>
With the dataset created in this ETL process, the user will be able to retrieve all kind of information from it. For example it is possible to query the best rated beers in Orlando, what kind of beer this brewery is offering as weLl as the rating for that beer and the alcooholic percentage on it, the beer reviews covers different aspects of the beer, so it can narrow down to the users specific needs. The user would be able to have the address and the website in hands in order to do further exploration. Given more time we would build an Flask appliation in order to provide a better interactivity feature for the data created.</br>

