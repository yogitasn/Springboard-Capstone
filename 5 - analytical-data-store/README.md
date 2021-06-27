#### Datawarehouse setup
Azure SQL Pool/Synapse Analytics offers various analytics engines to help you ingest, transform, model and analyze your data. A dedicated SQL pool offers T-SQL based compute and storage capabilities. After creating a dedicated SQL pool in your Synapse workspace, data can be loaded, modeled , procesed and delivered for faster analytic insight.


#### `Paid Occupancy Star Schema`
* dbo.paid_occupancy: Fact table for holding the paid parking records from year 2012 to 2020
* dbo.paid_occupancy_test1: Table for holding the paid parking records for the year 2021
* dbo.date_dim: Dimension table for holding the date dimension created from the fact table occupancydate timestamp.
* dbo.blockface: Table for holding the blockface records

#### Screenshot

## `BlockfaceTable`
![Alt text](../Documentation/BlockfaceTable.PNG?raw=true "BlockfaceTable")

## `BlockfaceTableRecordCount`
![Alt text](../Documentation/BlockfaceCount.PNG?raw=true "BlockfaceTableRecordCount")

## `DateDimensionTable`
![Alt text](../Documentation/datedimTable.PNG?raw=true "DateDimensionTable")

## `HistoricPaidOccupancyTable`
![Alt text](../Documentation/PaidOccupancyTable.PNG?raw=true "PaidOccupancyHistoricTable")

![Alt text](../Documentation/PaidOccupancyTable1.PNG?raw=true "PaidOccupancyHistoricTable")

## `NoOfDeltaRecordsBeforeLoad`
![Alt text](../Documentation/NoOfDeltaRecordsBeforeLoad.PNG?raw=true "NoOfDeltaRecordsBeforeLoad")


## `DeltaLoad`
#### Below is the table after executing `IncrementalDeltaLoadPerformance` pipeline in Data Factory. The no. of records in the processed folder for the year 2021 match with the no. of records after loading in the final table in SQL DWH.

![Alt text](../Documentation/DeltaLoad.PNG?raw=true "DeltaLoad")

