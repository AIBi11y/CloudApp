--Load data
listings = LOAD '/assignment2/listings.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER')  
AS(id:chararray, listing_url:chararray, scrape_id:chararray, last_scraped:chararray, name:chararray, summary:chararray, space:chararray, description:chararray, experiences_offered:chararray, neighborhood_overview:chararray, notes:chararray, transit:chararray, access:chararray, interaction:chararray, house_rules:chararray, thumbnail_url:chararray, medium_url:chararray, picture_url:chararray, xl_picture_url:chararray, host_id:chararray, host_url:chararray, host_name:chararray, host_since:chararray, host_location:chararray, host_about:chararray, host_response_time:chararray, host_response_rate:chararray, host_acceptance_rate:chararray, host_is_superhost:chararray, host_thumbnail_url:chararray, host_picture_url:chararray, host_neighbourhood:chararray, host_listings_count:chararray, host_total_listings_count:chararray, host_verifications:chararray, host_has_profile_pic:chararray, host_identity_verified, street:chararray, neighbourhood:chararray, neighbourhood_cleansed:chararray, neighbourhood_group_cleansed:chararray, city:chararray, state:chararray, zipcode:chararray, market:chararray, smart_location:chararray, country_code:chararray, country:chararray, latitude:chararray, longitude:chararray, is_location_exact:chararray, property_type:chararray, room_type:chararray, accommodates:chararray, bathrooms:chararray, bedrooms:chararray, beds:chararray, bed_type:chararray, amenities:chararray, square_feet:chararray, price:chararray, weekly_price:chararray, monthly_price:chararray, security_deposit:chararray, cleaning_fee:chararray, guests_included:chararray, extra_people:chararray, minimum_nights:chararray, maximum_nights:chararray, minimum_minimum_nights:chararray, maximum_minimum_nights:chararray, minimum_maximum_nights:chararray, maximum_maximum_nights:chararray, minimum_nights_avg_ntm:chararray, maximum_nights_avg_ntm:chararray, calendar_updated:chararray, has_availability:chararray, availability_30:chararray, availability_60:chararray, availability_90:chararray, availability_365:chararray, calendar_last_scraped:chararray, number_of_reviews:chararray, number_of_reviews_ltm:chararray, first_review:chararray, last_review:chararray, review_scores_rating:chararray, review_scores_accuracy:chararray, review_scores_cleanliness:chararray, review_scores_checkin:chararray, review_scores_communication:chararray, review_scores_location:chararray, review_scores_value:chararray, requires_license:chararray, license:chararray, jurisdiction_names:chararray, instant_bookable:chararray, is_business_travel_ready:chararray, cancellation_policy:chararray, require_guest_profile_picture:chararray, require_guest_phone_verification:chararray, calculated_host_listings_count:chararray, calculated_host_listings_count_entire_homes:chararray, calculated_host_listings_count_private_rooms:chararray, calculated_host_listings_count_shared_rooms:chararray, reviews_per_month:chararray);



final_output = foreach listings generate id
				, 	neighbourhood_cleansed
					--remove dollar sign, cast to int and add. If cleaning fee missing replace with 0
				,	(float) REPLACE (price,'\\\$','') +  (float) REPLACE (((cleaning_fee == '') ? '0': cleaning_fee), '\\\$','') as total_price
				,	property_type
				,	room_type
				,	review_scores_rating
				,	amenities
					--remove everything but commas from amenities and count remaining
					--to find number of amenities. Don't need +1 for some reason
				,	SIZE(REPLACE (amenities, '[^,]', ''))  as amenity_cnt
			;

STORE final_output into '/cleansed_data_assignment2' USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','NO_MULTILINE','NOCHANGE','SKIP_OUTPUT_HEADER');
