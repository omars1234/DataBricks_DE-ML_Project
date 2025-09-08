
CREATE TABLE IF NOT EXISTS BronzeHousePrices_info (
datesold  DATE,
postcode STRING,
price  STRING,
propertyType STRING,
bedrooms STRING
) ;


INSERT INTO BronzeHousePrices_info (datesold, postcode, price, propertyType, bedrooms)
  SELECT
    datesold,
    postcode,
    price,
    propertyType,
    bedrooms
  FROM
    raw_sales;