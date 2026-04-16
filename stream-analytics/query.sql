-- Cosmos DB Output
SELECT
    input.location AS location,
    AVG(input.iceThickness) AS avgIceThickness,
    AVG(input.waterTemperature) AS avgWaterTemperature,
    CASE
        WHEN AVG(input.iceThickness) >= 30 THEN 'Excellent'
        WHEN AVG(input.iceThickness) >= 20 THEN 'Good'
        WHEN AVG(input.iceThickness) >= 15 THEN 'Fair'
        ELSE 'Poor'
    END AS skatingCondition,
    System.Timestamp() AS timestamp,
    CONCAT(input.location, '-latest') AS id
INTO cosmosOutput
FROM input
GROUP BY input.location, TumblingWindow(second, 60);

-- Blob Storage Output
SELECT
    input.location,
    AVG(input.iceThickness) AS avgIceThickness,
    AVG(input.waterTemperature) AS avgWaterTemperature,
    CASE
        WHEN AVG(input.iceThickness) >= 30 THEN 'Excellent'
        WHEN AVG(input.iceThickness) >= 20 THEN 'Good'
        WHEN AVG(input.iceThickness) >= 15 THEN 'Fair'
        ELSE 'Poor'
    END AS skatingCondition,
    System.Timestamp() AS timestamp
INTO blobOutput
FROM input
GROUP BY input.location, TumblingWindow(second, 60);
