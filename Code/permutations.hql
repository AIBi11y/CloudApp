select distinct neighbourhood 
,property_type
,amenities_family_friendly_ind
,amenities_heating_ind
,amenities_washer_ind
,amenities_free_park_on_ind
,amenities_paid_park_off_ind
,amenities_kitchen_ind
,amenities_wifi_ind
,amenities_internet_ind
,amenities_cable_tv_ind
,amenities_tv_ind
,guests_included
,beds
,room_type

from (select neighbourhood from cleansed_listings)	bse1
cross join (select distinct  property_type from cleansed_listings)	bse2
cross join (select distinct  room_type from cleansed_listings)	bse3
cross join (select distinct  beds from cleansed_listings)	bse4
cross join (select distinct  guests_included from cleansed_listings)	bse5
cross join (select distinct  amenities_tv_ind from cleansed_listings)	bse6
cross join (select distinct  amenities_cable_tv_ind from cleansed_listings)	bse7
cross join (select distinct  amenities_internet_ind from cleansed_listings)	bse8
cross join (select distinct  amenities_wifi_ind from cleansed_listings)	bse9
cross join (select distinct  amenities_kitchen_ind from cleansed_listings)	bse10
cross join (select distinct  amenities_paid_park_off_ind from cleansed_listings)	bse11
cross join (select distinct  amenities_free_park_on_ind from cleansed_listings)	bse12
cross join (select distinct  amenities_heating_ind from cleansed_listings)	bse13
cross join (select distinct  amenities_family_friendly_ind from cleansed_listings)	bse14
cross join (select distinct  amenities_washer_ind from cleansed_listings)	bse15
;
