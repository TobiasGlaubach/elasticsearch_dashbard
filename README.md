
# Elasticsearch plotly Dashboard

  

This project is a simple dashboard build with python plotly dash for simple full text query of uploaded files to an elasticsearch server.

  

## Install

  

### Install Elasticsearch or change server address

  

Download and start the Elasticsearch server based on the information given on their webpage:

https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html

or change the connection address to use in the source files 
- `dash_main.py`
- `scrape_and_upload.py`.

  

### Install requirements.txt

  

    pip install -r requirements.txt

  

### Download and install spacy dictionary for NLP

  open the anaconda command prompt (or any console where python is in the PATH) and run the following line:

    python -m spacy download en_core_web_sm

  

### Run Scraping

run the scraping script to upload a directory (and it's sub directorories) to elasticsearch.

### Start Plotly Dashboard

open the anaconda command prompt (or any console where python is in the PATH) and run the following line:

    python.exe dash_main.py

### Open your Dashboard

Follow the link shown on startup of the python app.

Usually something like:
http://127.0.0.1:8050
