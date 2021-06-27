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
 
 CREATE TABLE dbo.paid_occupancy_test1
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
         [latitude] float,
         [longitude] float,
         [station_id] int,
        CONSTRAINT PK_Occupancy PRIMARY KEY NONCLUSTERED (occupancydatetime,latitude,longitude) NOT ENFORCED
    	)
WITH
(   CLUSTERED COLUMNSTORE INDEX
,  DISTRIBUTION = HASH([station_id])
)
 GO

CREATE TABLE dbo.Blockface
 	(
 	 [station_id] int,
 	 [station_address] varchar(1000),
 	 [side] varchar(2),
 	 [block_nbr] int,
 	 [parking_category] varchar(200),
 	 [wkd_rate1] float,
 	 [wkd_start1] varchar(100),
 	 [wkd_end1] varchar(100),
 	 [wkd_rate2] float,
 	 [wkd_start2] varchar(100),
 	 [wkd_end2] varchar(100),
     [wkd_rate3] float,
 	 [wkd_start3] varchar(100),
 	 [wkd_end3] varchar(100),
     [sat_rate1] float,
 	 [sat_start1] varchar(100),
 	 [sat_end1] varchar(100),
 	 [sat_rate2] float,
 	 [sat_start2] varchar(100),
 	 [sat_end2] varchar(100),
     [sat_rate3] float,
 	 [sat_start3] varchar(100),
 	 [sat_end3] varchar(100),
     [parking_time_limit] int,
     [subarea] varchar(100),
     CONSTRAINT PK_Station_ID PRIMARY KEY NONCLUSTERED (station_id) NOT ENFORCED
 	)
 WITH
 	(
 	DISTRIBUTION = REPLICATE,
 	CLUSTERED COLUMNSTORE INDEX
 	 -- HEAP
 	)
 GO

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
