To run this app. do the following:

Go to root project folder.
Evaluate following commands:

```
docker build -t trg_docker .
docker run -it -d -v {YOUR_LOCAL_FOLDER}:/TRG/resources/output --name denis_trg trg_docker
```
Where {YOUR_LOCAL_FOLDER} is a folder on your laptop for mount docker results

---------------------------------------------------------------------------------
To evaluate kpi's run inside the docker container
```
python3 src/processing/kpi.py --metrics {PREDEFINED_KPI_LIST}
```
Where {PREDEFINED_KPI_LIST} is a list of kpi's delimited by comma.
I implemented following kpis:
* top_5_crime_districts
* top_5_safe_districts
* crimes_by_crime_type
* different_crimes
* different_crime_types
* top_5_crime_types_by_district

You can pass any of them inside the {PREDEFINED_KPI_LIST} by names.
For example:
```
python3 src/processing/kpi.py --metrics different_crime_types,top_5_crime_districts
```
The kpi results will be in {YOUR_LOCAL_FOLDER}/kpi

---------------------------------------------------------------------------------
To load samples of the whole datasets you can use dataset loader:
```
python3 src/processing/dataset_loader.py --districts {districts} --crime_types {crime_types} --outcomes {outcomes}
```
Where districts, crime_types and outcomes are lists of the values of the respective fields if we want to create the subset of our data. You can not pass any filters if you want to load all dataset.
The result will be in {YOUR_LOCAL_FOLDER}/data


