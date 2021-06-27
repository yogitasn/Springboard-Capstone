#### Datawarehouse setup
Azure SQL Pool/Synapse Analytics offers various analytics engines to help you ingest, transform, model and analyze your data. A dedicated SQL pool offers T-SQL based compute and storage capabilities. After creating a dedicated SQL pool in your Synapse workspace, data can be loaded, modeled , procesed and delivered for faster analytic insight.


#### `commands.sql`
TSQL script to create following tables in a star-schema. The scripts need to be executed one-time in Microsoft SQL Management Studio or Query Editor under Dedicated SQL Pool in Azure Portal.

* dbo.paid_occupancy: Fact table for holding the paid parking records from year 2012 to 2020
* dbo.paid_occupancy_test1: Table for holding the paid parking records for the year 2021
* dbo.date_dim: Dimension table for holding the date dimension created from the fact table occupancydate timestamp.
* dbo.blockface: Table for holding the blockface records

<hr/>

Hash-distributed tables work well for large fact tables in a star schema. They can have very large numbers of rows and still achieve high performance. There are, of course, some design considerations that help you to get the performance the distributed system is designed to provide. Choosing a good distribution column is one such consideration that is described in this article.

```
CREATE TABLE dbo.paid_occupancy
 	(
 	 [occupancydatetime] datetime not null,
         [paidoccupancy] int,
         [blockfacename] varchar(1000),
         [sideofstreet] varchar(2),
         [parkingtimelimitcategory] int,
         [available_spots] int,
         [paidparkingarea] varchar(100),
         [paidparkingsubarea] varchar(100),
         [paidparkingrate] float,
         [parkingcategory] varchar(50),
         [latitude] decimal(10,6),
         [longitude] decimal(10,6),
         [station_id] int,
        CONSTRAINT PK_Occupancy PRIMARY KEY NONCLUSTERED (occupancydatetime,latitude,longitude) NOT ENFORCED
    	)
WITH
(   CLUSTERED COLUMNSTORE INDEX
,  DISTRIBUTION = HASH([station_id])
)
 GO
```
<hr/>

<hr/>
Replicated tables work well for dimension tables in a star schema. Dimension tables are typically joined to fact tables which are distributed differently than the dimension table. Dimensions are usually of a size that makes it feasible to store and maintain multiple copies. Dimensions store descriptive data that changes slowly, such as customer name and address, and product details. The slowly changing nature of the data leads to less maintenance of the replicated table.

```
CREATE TABLE dbo.date_dim
 	(
 	 [occupancydatetime] datetime not null,
 	 [day_of_week] varchar(20),
	 [monthname] varchar(20),
	 CONSTRAINT PK_OccupancyDateTime PRIMARY KEY NONCLUSTERED (occupancydatetime) NOT ENFORCED
 		)
 WITH
 	(
 	DISTRIBUTION = REPLICATE,
 	 CLUSTERED COLUMNSTORE INDEX
 	 -- HEAP
 	)
```
<hr/>