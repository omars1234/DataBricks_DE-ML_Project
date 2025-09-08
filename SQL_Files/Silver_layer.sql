CREATE TABLE IF NOT EXISTS SilverHousePrices_info (
  Date_Sold DATE,
  Region_Postcode STRING,
  Property_Type STRING,
  Number_Of_Bedrooms INT,
  price INT,
  Year_DATE INT,
  Month_Name STRING,
  Day_Name STRING,
  Average_Price_per_Bedroom DOUBLE
);

TRUNCATE TABLE SilverHousePrices_info;
     
INSERT INTO SilverHousePrices_info (
    Date_Sold,
    Region_Postcode,
    Property_Type,
    Number_Of_Bedrooms,
    Price,
    Year_DATE,
    Month_Name,
    Day_Name,
    Average_Price_per_Bedroom
  )
  SELECT
    datesold AS Date_Sold,
    TRIM(postcode) AS Region_Postcode,
    TRIM(propertyType) AS Property_Type,
    cast(bedrooms AS int) AS Number_Of_Bedrooms,
    cast(price AS int) AS Price,
    YEAR(Date_Sold) As Year_DATE,
    date_format(Date_Sold, 'MMMM') AS Month_Name,
    dayofmonth(Date_Sold) As Day_month,
    Price / NULLIF(Number_Of_Bedrooms, 0) As Average_Price_per_Bedroom
  FROM
    BronzeHousePrices_info;