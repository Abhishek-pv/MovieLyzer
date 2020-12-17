**Analysis of Movie Data Using Spark**

****Introduction****

After briefly going through the movie dataset, one can start to notice some correlations or trends between various characteristics of the movie. The pertinent business question that any Data Analyst would ask when browsing through this data set is to find out what characteristics of movies produce the highest revenue. This investigation and research on the trends of the data would help the film industry to work on those characteristics that would generate the highest revenue.  

****Dataset**** 

The dataset for this analysis is taken from Kaggle and below is the link for the same, 

https://www.kaggle.com/rounakbanik/the-movies-dataset 

￼These files contain metadata for all 45,000 movies listed in the Full Movie Lens Dataset. The dataset consists of movies released on or before July 2017. This dataset also has files containing 26 million ratings from 270,000 users for all 45,000 movies. Ratings are on a scale of 1-5. 

This dataset consists of the following files: 

movies_metadata.csv: The main Movies Metadata file. Contains information on 45,000 movies featured in the Full MovieLens dataset. Features include posters, backdrops, budget, revenue, release dates, languages, production countries and companies. 

credits.csv: Consists of Cast and Crew Information for all our movies. Available in the form of a stringified JSON Object. 

ratings_small.csv: The subset of 100,000 ratings from 700 users. 

 

Other challenge using this data set was the missing values and many fields were in JSON multiple dictionary format and these needed to be cleaned to keep only the relevant information with us. 

The cleaned data has been loaded to the Cassandra tables movies_etl, ratings, and credits under the keyspace code500. 



**Requirements** 

* Spark 2.4+ 
* Python 3.5+ 
* Bootstrap 3 
* HTML5 
* CSS3
* Javascript ECMAScript6  
* Tableau 2019.4 
* AWS EMR


**Cluster instructions**


We have done all our analysis by connecting remotely to SFU cluster- gateway.sfucloud.ca by SSH using the command below, 

ssh <userid>@gateway.sfucloud.ca 

 

**Cassandra Instructions**

 

As we have two Cassandra clusters available, we are utilizing the reliable cluster- 199.60.17.32. 

We can connect to the cluster using the below command, 

cqlsh --cqlversion=3.4.4 199.60.17.32 

We have created a common keyspace code500 within this Cassandra cluster which can be accessed using the command, 

Use code500; 

We have created 4 tables- movies_etl, ratings and credits within this keyspace. 

 

**Analysis**

We have written various programs using pyspark for each analysis and the description & running instructions is provided in the Running.txt file. 


**Results**

The output for each of the program will be stored as an output csv file. 

**Frontend** 

 
We have created a user interface “Movielyzer” that provides the user various options to analyze various aspects of a movie and its visualization.   

URL: https://movielyzerbigdata.000webhostapp.com/ 

Use standard browsers like Google Chrome or FireFox. 

 

**Visualisation** 

All the obtained results are visualized using Tableau. All the visualizations have been embedded to the website using JavaScript API. 
