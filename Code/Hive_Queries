-- Pig output is stored in A2 directory: 
-- STORE final_output INTO '/A2' USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','NO_MULTILINE','NOCHANGE','SKIP_OUTPUT_HEADER');

-- View existing tables
SHOW tables;

-- Print column headers
SET hive.cli.print.header=true;

-- Create a table holding the reduced number of columns
CREATE TABLE listings2 (Id Int, Neighbourhood String, Price float, PropertyType String, RoomType String, ReviewScoresRating Int, Amenities String, AmenityCount Int, Latitude String, Longitude String) row format delimited FIELDS TERMINATED BY '|' location '/A2';

-- Check out the newly created table
DESCRIBE listings2;

-- Check if all records are unique:
SELECT COUNT(*), COUNT(DISTINCT ID) FROM LISTINGS2;
-- Two counts should be equal

-- Number of listings per Neighbourhood
SELECT NEIGHBOURHOOD, COUNT(*) FROM LISTINGS2 GROUP BY NEIGHBOURHOOD;

-- Average, Minimum and Maximum Listing Price
SELECT ROUND(AVG(PRICE)), ROUND(MIN(PRICE)), ROUND(MAX(PRICE)) FROM LISTINGS2;

-- Average, Minimum and Maximum Listing Price per Neighbourhood
SELECT NEIGHBOURHOOD, ROUND(AVG(PRICE)), ROUND(MIN(PRICE)), ROUND(MAX(PRICE)) FROM LISTINGS2 GROUP BY NEIGHBOURHOOD;

-- Average number of Amenities
SELECT ROUND(AVG(amenitycount)) FROM LISTINGS2;

-- Average number of Amenities per Neighbourhood
SELECT NEIGHBOURHOOD, ROUND(AVG(amenitycount)) FROM LISTINGS2 GROUP BY NEIGHBOURHOOD;

-- Average Review Score
SELECT ROUND(AVG(reviewscoresrating)) FROM LISTINGS2;

-- Average Review Score per Neighbourhood
SELECT NEIGHBOURHOOD, ROUND(AVG(reviewscoresrating)) FROM LISTINGS2 GROUP BY NEIGHBOURHOOD;

-- Total number of Reviews
SELECT COUNT(reviewscoresrating) FROM LISTINGS2;

-- Number of distinct Review Score Ratings
SELECT COUNT(DISTINCT reviewscoresrating) FROM LISTINGS2;


-- Number of listings per Room Type
SELECT ROOMTYPE, COUNT(*) FROM LISTINGS2 GROUP BY ROOMTYPE;

-- Number of listings per Neighbourhood and Room Type
SELECT NEIGHBOURHOOD, ROOMTYPE, COUNT(*) FROM LISTINGS2 GROUP BY NEIGHBOURHOOD, ROOMTYPE;

-- Number of Property Types
SELECT PROPERTYTYPE, COUNT(*) FROM LISTINGS2 GROUP BY PROPERTYTYPE;
