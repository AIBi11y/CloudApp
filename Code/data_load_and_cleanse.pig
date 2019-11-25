--Load data
listings = LOAD '/assignment2/listings.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER')  
AS(id:chararray, listing_url:chararray, scrape_id:chararray, last_scraped:chararray, name:chararray, summary:chararray, space:chararray, description:chararray, experiences_offered:chararray, neighborhood_overview:chararray, notes:chararray, transit:chararray, access:chararray, interaction:chararray, house_rules:chararray, thumbnail_url:chararray, medium_url:chararray, picture_url:chararray, xl_picture_url:chararray, host_id:chararray, host_url:chararray, host_name:chararray, host_since:chararray, host_location:chararray, host_about:chararray, host_response_time:chararray, host_response_rate:chararray, host_acceptance_rate:chararray, host_is_superhost:chararray, host_thumbnail_url:chararray, host_picture_url:chararray, host_neighbourhood:chararray, host_listings_count:chararray, host_total_listings_count:chararray, host_verifications:chararray, host_has_profile_pic:chararray, host_identity_verified, street:chararray, neighbourhood:chararray, neighbourhood_cleansed:chararray, neighbourhood_group_cleansed:chararray, city:chararray, state:chararray, zipcode:chararray, market:chararray, smart_location:chararray, country_code:chararray, country:chararray, latitude:chararray, longitude:chararray, is_location_exact:chararray, property_type:chararray, room_type:chararray, accommodates:chararray, bathrooms:chararray, bedrooms:int, beds:int, bed_type:chararray, amenities:chararray, square_feet:chararray, price:chararray, weekly_price:chararray, monthly_price:chararray, security_deposit:chararray, cleaning_fee:chararray, guests_included:int, extra_people:chararray, minimum_nights:chararray, maximum_nights:chararray, minimum_minimum_nights:chararray, maximum_minimum_nights:chararray, minimum_maximum_nights:chararray, maximum_maximum_nights:chararray, minimum_nights_avg_ntm:chararray, maximum_nights_avg_ntm:chararray, calendar_updated:chararray, has_availability:chararray, availability_30:chararray, availability_60:chararray, availability_90:chararray, availability_365:chararray, calendar_last_scraped:chararray, number_of_reviews:chararray, number_of_reviews_ltm:chararray, first_review:chararray, last_review:chararray, review_scores_rating:chararray, review_scores_accuracy:chararray, review_scores_cleanliness:chararray, review_scores_checkin:chararray, review_scores_communication:chararray, review_scores_location:chararray, review_scores_value:chararray, requires_license:chararray, license:chararray, jurisdiction_names:chararray, instant_bookable:chararray, is_business_travel_ready:chararray, cancellation_policy:chararray, require_guest_profile_picture:chararray, require_guest_phone_verification:chararray, calculated_host_listings_count:chararray, calculated_host_listings_count_entire_homes:chararray, calculated_host_listings_count_private_rooms:chararray, calculated_host_listings_count_shared_rooms:chararray, reviews_per_month:chararray);



final_output = foreach listings generate id
				, 	neighbourhood_cleansed
					--remove dollar sign, cast to int and add. If cleaning fee missing replace with 0
				,	(float) REPLACE (price,'\\\$','') +  (float) REPLACE (((cleaning_fee == '') ? '0': cleaning_fee), '\\\$','') as total_price
				,	property_type
				,	room_type
				,	beds 
				,	review_scores_rating
				,	amenities
				,	guests_included
					--remove everything but commas from amenities and count remaining
					--to find number of amenities. Don't need +1 for some reason
				,	SIZE(REPLACE (amenities, '[^,]', ''))  as amenity_cnt
					
					--create some indicators for amenities to model on
				, ((amenities matches '.*TV.*') ? 1:0 ) 	AS amenities_tv_ind
				, ((amenities matches '.*Cable TV.*') ? 1:0 ) AS amenities_cable_tv_ind
				, ((amenities matches '.*Internet.*') ? 1:0 ) AS amenities_internet_ind
				, ((amenities matches '.*Wifi.*') ? 1:0 ) AS amenities_wifi_ind
				, ((amenities matches '.*Kitchen.*') ? 1:0 ) AS amenities_kitchen_ind
				, ((amenities matches '.*Paid parking off premises.*') ? 1:0 ) AS amenities_paid_park_off_ind
				, ((amenities matches '.*Free parking on premises.*') ? 1:0 ) AS amenities_free_park_on_ind
				, ((amenities matches '.*Heating.*') ? 1:0 ) AS amenities_heating_ind
				, ((amenities matches '.*Family/kid friendly.*') ? 1:0 ) AS amenities_family_friendly_ind
				, ((amenities matches '.*Washer.*') ? 1:0 ) AS amenities_washer_ind
				, ((amenities matches '.*Dryer.*') ? 1:0 ) AS amenities_dryer_ind
				, ((amenities matches '.*Private entrance.*') ? 1:0 ) AS amenities_priv_entrance_ind
				, ((amenities matches '.*Bathtub.*') ? 1:0 ) AS amenities_bathtub_ind
				, ((amenities matches '.*Coffee maker.*') ? 1:0 ) AS amenities_coffee_maker_ind
				, ((amenities matches '.*Garden or backyard.*') ? 1:0 ) AS amenities_garden_ind
				, ((amenities matches '.*Free street parking.*') ? 1:0 ) AS amenities_park_free_street_ind
				, ((amenities matches '.*24-hour check-in.*') ? 1:0 ) AS amenities_24hr_checkin_ind
				, ((amenities matches '.*Breakfast.*') ? 1:0 ) AS amenities_breakfast_ind
				, ((amenities matches '.*Smoking allowed.*') ? 1:0 ) AS amenities_smoking_allowed_ind
				, ((amenities matches '.*Pets allowed.*') ? 1:0 ) AS amenities_pets_allowed_ind
				, ((amenities matches '.*En suite bathroom.*') ? 1:0 ) AS amenities_en_suite_ind
				, ((amenities matches '.*Dishwasher.*') ? 1:0 ) AS amenities_dishwasher_ind
				, ((amenities matches '.*Lockbox.*') ? 1:0 ) AS amenities_lockbox_ind
				, ((amenities matches '.*Microwave.*') ? 1:0 ) AS amenities_microwave_ind
				, ((amenities matches '.*Refrigerator.*') ? 1:0 ) AS amenities_fridge_ind
				, ((amenities matches '.*Oven.*') ? 1:0 ) AS amenities_oven_ind
				, ((amenities matches '.*Stove.*') ? 1:0 ) AS amenities_stove_ind
				, ((amenities matches '.*Balcony.*') ? 1:0 ) AS amenities_balcony_ind
				, ((amenities matches '.*Terrace.*') ? 1:0 ) AS amenities_terrace_ind
				, ((amenities matches '.*Self check-in.*') ? 1:0 ) AS amenities_self_checkin_ind
				, ((amenities matches '.*Host greets you.*') ? 1:0 ) AS amenities_host_meets_you_ind
				, ((amenities matches '.*Air conditioning.*') ? 1:0 ) AS amenities_air_con_ind
			;

STORE final_output into '/cleansed_data_assignment2' USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','NO_MULTILINE','NOCHANGE','SKIP_OUTPUT_HEADER');

