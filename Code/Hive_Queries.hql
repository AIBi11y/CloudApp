-- Pig output is stored in cleansed_data directory: 
-- STORE final_output INTO '/cleansed_data' USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','NO_MULTILINE','NOCHANGE','SKIP_OUTPUT_HEADER');

-- View existing tables
SHOW tables;

-- Print column headers
SET hive.cli.print.header=true;

-- Create a table holding the reduced number of columns
CREATE TABLE reduced_listings (id int, neighbourhood_cleansed String, total_price float
, property_type String, room_type String, beds int, review_scores_rating int, amenities String, guests_included int
, amenity_cnt int
, amenities_tv_ind int
, amenities_cable_tv_ind int
, amenities_internet_ind int
, amenities_wifi_ind int
, amenities_kitchen_ind int
, amenities_paid_park_off_ind int
, amenities_free_park_on_ind int
, amenities_heating_ind int
, amenities_family_friendly_ind int
, amenities_washer_ind int
, amenities_dryer_ind int
, amenities_priv_entrance_ind int
, amenities_bathtub_ind int
, amenities_coffee_maker_ind int
, amenities_garden_ind int
, amenities_park_free_street_ind int
, amenities_24hr_checkin_ind int
, amenities_breakfast_ind int
, amenities_smoking_allowed_ind int
, amenities_pets_allowed_ind int
, amenities_en_suite_ind int
, amenities_dishwasher_ind int
, amenities_lockbox_ind int
, amenities_microwave_ind int
, amenities_fridge_ind int
, amenities_oven_ind int
, amenities_stove_ind int
, amenities_balcony_ind int
, amenities_terrace_ind int
, amenities_self_checkin_ind int
, amenities_host_meets_you_ind int
, amenities_air_con_ind int
, latitude String
, longitude String) 
row format delimited 
fields terminated by '|' 
location '/cleansed_data';

-- Check out the newly created table
DESCRIBE reduced_listings;

-- Check if all records are unique:
SELECT COUNT(*), COUNT(DISTINCT ID) FROM reduced_listings;
-- Two counts should be equal

-- Number of listings per Neighbourhood
SELECT neighbourhood_cleansed, COUNT(*) 
FROM reduced_listings GROUP BY neighbourhood_cleansed;

-- Average, Minimum and Maximum Listing Price
SELECT ROUND(AVG(total_price)), ROUND(MIN(total_price)), ROUND(MAX(total_price)) 
FROM reduced_listings;

-- Average, Minimum and Maximum Listing Price per Neighbourhood
SELECT neighbourhood_cleansed, ROUND(AVG(total_price)), ROUND(MIN(total_price)), ROUND(MAX(total_price)) 
FROM reduced_listings GROUP BY neighbourhood_cleansed;

-- Average total price per Neighbourhood
SELECT neighbourhood_cleansed, ROUND(AVG(total_price)) 
FROM reduced_listings GROUP BY neighbourhood_cleansed;

-- Check if the free parking makes a difference to the listing's total price:
SELECT neighbourhood_cleansed, ROUND(AVG(total_price)) 
FROM reduced_listings 
WHERE amenities_free_park_on_ind = 1 
GROUP BY neighbourhood_cleansed;

-- Average total price per Neighbourhood, depending on the number of amenities listed
SELECT neighbourhood_cleansed, amenity_cnt, ROUND(AVG(total_price))
FROM reduced_listings GROUP BY neighbourhood_cleansed, amenity_cnt;

-- Average number of Amenities
SELECT ROUND(AVG(amenity_cnt)) FROM reduced_listings;

-- Average number of Amenities per Neighbourhood
SELECT neighbourhood_cleansed, ROUND(AVG(amenity_cnt)) 
FROM reduced_listings GROUP BY neighbourhood_cleansed;

-- Average Review Score
SELECT ROUND(AVG(review_scores_rating)) FROM reduced_listings;

-- Average Review Score per Neighbourhood
SELECT neighbourhood_cleansed, ROUND(AVG(review_scores_rating)) 
FROM reduced_listings GROUP BY neighbourhood_cleansed;

-- Total number of Reviews
SELECT COUNT(review_scores_rating) FROM reduced_listings;

-- Number of distinct Review Score Ratings
SELECT COUNT(DISTINCT review_scores_rating) FROM reduced_listings;

-- Number of listings per Room Type
SELECT room_type, COUNT(*) FROM reduced_listings GROUP BY room_type;

-- Number of listings per Neighbourhood and Room Type
SELECT neighbourhood_cleansed, room_type, COUNT(*) 
FROM reduced_listings GROUP BY neighbourhood_cleansed, room_type;

-- Number of Property Types
SELECT property_type, COUNT(*) FROM reduced_listings GROUP BY property_type;

-- Average Number of Beds per Neighbourhood
SELECT neighbourhood_cleansed, ROUND(AVG(beds)), MAX(beds), MIN(beds) 
FROM reduced_listings GROUP BY neighbourhood_cleansed;
