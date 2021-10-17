-- 1. Создание таблицы ОС
CREATE SEQUENCE os_id_seq;
CREATE TABLE IF NOT EXISTS public.os
(
	id INT NOT NULL DEFAULT nextval('os_id_seq'),
	name TEXT NOT NULL,
	vendor TEXT NOT NULL
);
ALTER SEQUENCE os_id_seq OWNED BY public.os.id;

-- 2. Создание таблицы компаний
CREATE SEQUENCE companies_id_seq;
CREATE TABLE IF NOT EXISTS public.companies
(
	id INT NOT NULL DEFAULT nextval('companies_id_seq'),
	name TEXT NOT NULL,
	location TEXT NOT NULL
);
ALTER SEQUENCE companies_id_seq OWNED BY public.companies.id;

-- 3. Создание таблицы устройств
CREATE SEQUENCE devices_id_seq;
CREATE TABLE IF NOT EXISTS public.devices
(
	id INT NOT NULL DEFAULT nextval('devices_id_seq'),
	name TEXT NOT NULL,
	company_id INT NOT NULL,
	os_id INT NOT NULL
);
ALTER SEQUENCE devices_id_seq OWNED BY public.devices.id;

-- 4. Создание таблицы магазинов
CREATE SEQUENCE shops_id_seq;
CREATE TABLE IF NOT EXISTS public.shops
(
	id INT NOT NULL DEFAULT nextval('shops_id_seq'),
	name TEXT NOT NULL,
	phone TEXT NOT NULL
);
ALTER SEQUENCE shops_id_seq OWNED BY public.shops.id;

-- 5. Создание общего прайс-листа
CREATE SEQUENCE prices_id_seq;
CREATE TABLE IF NOT EXISTS public.prices
(
	id INT NOT NULL DEFAULT nextval('prices_id_seq'),
	shop_id INT NOT NULL,
	device_id INT NOT NULL,
	value TEXT NOT NULL
);
