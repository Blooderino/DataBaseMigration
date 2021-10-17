-- Create not normalized DB
CREATE TABLE device_catalog 
(
	company_location TEXT DEFAULT 'Unknown location' NOT NULL,
	company_name TEXT DEFAULT 'Unknown company' NOT NULL,
	device_name TEXT DEFAULT 'Unknown device' NOT NULL,
	device_price TEXT DEFAULT 'Unknown price' NOT NULL,
	os_name TEXT DEFAULT 'Unknown OS name' NOT NULL, 
	os_vendor TEXT DEFAULT 'Unknown OS version' NOT NULL, 
	shop_name TEXT DEFAULT 'Unknown shop' NOT NULL,
	shop_phone TEXT DEFAULT 'Unknown phone' NOT NULL
);