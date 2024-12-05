-- Create coffee show database
CREATE DATABASE CoffeeShop;

-- Select coffee shop database
USE CoffeeShop;

-- Create customer table
CREATE TABLE dbo.Customer
(
	customer_id INT NOT NULL PRIMARY KEY,
	home_store INT NOT NULL,
	[customer_first-name] NVARCHAR(50) NOT NULL,
	customer_email NVARCHAR(50) NOT NULL,
	customer_since DATE NOT NULL,
	loyalty_card_number NVARCHAR(50) NOT NULL,
	birthdate DATE NOT NULL,
	gender CHAR(1) NOT NULL,
	birth_year INT NOT NULL
);

-- Create generation table
CREATE TABLE dbo.Generation
(
	birth_year INT NOT NULL,
	generation NVARCHAR(20) NOT NULL
)

-- Create product table
CREATE TABLE dbo.Product
(
	product_id INT NOT NULL PRIMARY KEY,
	product_group NVARCHAR(50) NOT NULL,
	product_category NVARCHAR(50) NOT NULL,
	product_type NVARCHAR(50) NOT NULL,
	product NVARCHAR(50) NOT NULL,
	product_description TEXT NOT NULL,
	unit_of_measure NVARCHAR(10) NOT NULL,
	current_wholesale_price INT NOT NULL,
	current_retail_price FLOAT NOT NULL,
	tax_exempt_yn CHAR(1) NOT NULL,
	promo_yn CHAR(1) NOT NULL,
	new_product_yn CHAR(1) NOT NULL
);

-- Create date table
CREATE TABLE dbo.[Date]
(
	transaction_date DATE NOT NULL,
	Date_ID INT NOT NULL PRIMARY KEY,
	Week_ID INT NOT NULL,
	Week_Desc NVARCHAR(15) NOT NULL,
	Month_ID INT,
	Month_Name NVARCHAR(10) NOT NULL,
	Quarter_ID INT NOT NULL,
	Quarter_Name CHAR(2) NOT NULL,
	Year_ID INT NOT NULL
);

-- Create pastry inventory table
CREATE TABLE dbo.[Pastry Inventory]
(
	sales_outlet_id INT NOT NULL,
	transaction_date DATE NOT NULL,
	product_id INT NOT NULL,
	start_of_day INT NOT NULL,
	quantity_sold INT NOT NULL,
	waste INT NOT NULL,
	[% waste] FLOAT NOT NULL
);

-- Create sales target table
CREATE TABLE dbo.[Sales Target]
(
	sales_outlet_id INT NOT NULL,
	year_month DATE NOT NULL,
	beans_goal INT NOT NULL,
	beverage_goal INT NOT NULL,
	food_goal INT NOT NULL,
	merchandise_goal INT NOT NULL,
	total_goal INT NOT NULL
);

-- Create sales outlet table
CREATE TABLE dbo.[Sales Outlet]
(
	sales_outlet_id INT NOT NULL PRIMARY KEY,
	sales_outlet_type NVARCHAR(10) NOT NULL,
	store_square_feet INT NOT NULL,
	store_address NVARCHAR(20) NOT NULL,
	store_city NVARCHAR(20) NOT NULL,
	store_state_province CHAR(2) NOT NULL,
	store_telephone NVARCHAR(20) NOT NULL,
	store_postal_code INT NOT NULL,
	store_longitude INT NOT NULL,
	store_latitude INT NOT NULL,
	manager INT NOT NULL,
	Neighorhood NVARCHAR(20) NOT NULL
);

-- Create staff table
CREATE TABLE dbo.Staff
(
	staff_id INT NOT NULL PRIMARY KEY,
	first_name NVARCHAR(20) NOT NULL,
	last_name NVARCHAR(20) NOT NULL,
	position NVARCHAR(50) NOT NULL,
	[start_date] DATE NOT NULL,
	[location] NVARCHAR(10) NOT NULL
);

-- Create sales receipt table
CREATE TABLE dbo.[Sales Receipt]
(
	transaction_id INT NOT NULL,
	transaction_date DATE NOT NULL,
	transaction_time TIME NOT NULL,
	sales_outlet_id INT NOT NULL,
	staff_id INT NOT NULL,
	customer_id INT NOT NULL,
	instore_yn CHAR(1) NOT NULL,
	[order] INT NOT NULL,
	line_item_id INT NOT NULL,
	product_id INT NOT NULL,
	quantity INT NOT NULL,
	line_item_amount INT NOT NULL,
	unit_price INT NOT NULL,
	promo_item_yn CHAR(1) NOT NULL
);

-- Create SQL query to take dataset for OLAB Multidimensional Tabular Cube
SELECT
	transaction_id,
	transaction_date,
	Quarter_Name,
	Month_Name,
	product_id,
	product_group,
	product_category,
	product_type,
	product,
	product_description,
	tax_exempt_yn,
	promo_yn,
	new_product_yn,
	customer_id,
	home_store,
	[customer_first-name],
	customer_email,
	customer_since,
	loyalty_card_number,
	birthdate,
	gender,
	birth_year,
	generation,
	sales_outlet_id,
	staff_id,
	staff_name,
	position,
	sales_outlet_type,
	store_city,
	store_state_province,
	store_address,
	store_postal_code,
	store_telephone,
	store_latitude,
	store_longitude,
	Neighorhood,
	manager,
	quantity_sold,
	start_of_day,
	waste,
	[% waste],
	beans_goal,
	beverage_goal,
	food_goal,
	merchandise_goal,
	total_goal,
	store_square_feet,
	unit_of_measure,
	current_wholesale_price,
	current_retail_price
FROM (
SELECT
	sr.transaction_id,
	sr.transaction_date,
	dt.Quarter_Name,
	dt.Month_Name,
	prod.product_id,
	prod.product_group,
	prod.product_category,
	prod.product_type,
	prod.product,
	prod.product_description,
	prod.tax_exempt_yn,
	prod.promo_yn,
	prod.new_product_yn,
	cr.customer_id,
	cr.home_store,
	cr.[customer_first-name],
	cr.customer_email,
	cr.customer_since,
	cr.loyalty_card_number,
	cr.birthdate,
	cr.gender,
	cr.birth_year,
	gn.generation,
	sr.sales_outlet_id,
	sf.staff_id,
	CONCAT(sf.first_name, ' ', sf.last_name) AS staff_name,
	sf.position,
	st.sales_outlet_type,
	st.store_city,
	st.store_state_province,
	st.store_address,
	st.store_telephone,
	st.store_postal_code,
	st.Neighorhood,
	st.store_latitude,
	st.store_longitude,
	st.store_square_feet,
	st.manager,
	past.quantity_sold,
	past.start_of_day,
	past.waste,
	past.[% waste],
	tar.beans_goal,
	tar.beverage_goal,
	tar.food_goal,
	tar.merchandise_goal,
	tar.total_goal,
	prod.unit_of_measure,
	prod.current_wholesale_price,
	prod.current_retail_price,
	ROW_NUMBER() OVER (PARTITION BY sr.transaction_id
	ORDER BY sr.transaction_id) AS rn
FROM CoffeeShop.dbo.[Sales Receipt] sr
LEFT JOIN CoffeeShop.dbo.Product prod ON sr.product_id = prod.product_id
LEFT JOIN CoffeeShop.dbo.[Date] dt ON sr.transaction_date = dt.transaction_date
LEFT JOIN CoffeeShop.dbo.Customer cr ON sr.customer_id = cr.customer_id
LEFT JOIN CoffeeShop.dbo.Generation gn ON cr.birth_year = gn.birth_year
LEFT JOIN CoffeeShop.dbo.[Sales Outlet] st ON sr.sales_outlet_id = st.sales_outlet_id
LEFT JOIN CoffeeShop.dbo.[Pastry Inventory] past ON st.sales_outlet_id = past.sales_outlet_id
LEFT JOIN CoffeeShop.dbo.[Sales Target] tar ON st.sales_outlet_id = tar.sales_outlet_id
LEFT JOIN CoffeeShop.dbo.Staff sf ON sr.staff_id = sf.staff_id
) AS x
WHERE rn = 1
ORDER BY transaction_id;