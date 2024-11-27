create or replace view SITEMAP.RESI_GOLD.NEWEST_LISTINGS_BY_STATE_JSON(
	OBJECTKEY,
	JSONDATA,
	OBJECTCOUNT
) as WITH IS_MEMBER_LISTING_DATA AS (
        SELECT DISTINCT
            apd.PROPERTY_ID
            ,ARRAY_CONTAINS(true, ARRAY_AGG((AGENT.VALUE:isMember = true AND (AGENT.VALUE:role = 1 OR AGENT.VALUE:role = 3)))) AS ISMEMBERLISTING
        FROM
            SITEMAP.RESI_SILVER.ACTIVE_PROPERTY_DETAIL apd
            ,LATERAL FLATTEN(INPUT => AGENTS) AGENTS
            ,LATERAL FLATTEN(INPUT => AGENTS.VALUE:agents) AGENT
        GROUP BY
            apd.PROPERTY_ID
    )
        
    ,FORSALE_ACTIVE_PROPERTY_DETAIL AS (
        SELECT DISTINCT
            apd.STATE_ID
            ,TO_ARRAY(PRIMARY_LISTING.VALUE) AS PRIMARY_LISTINGS
            ,apd.PROPERTY_ID
            ,apd.CITY
            ,apd.STATE
            ,apd.ZIP_CODE
            ,apd.FULL_STREET_ADDRESS
            ,apd.STREET_NAME
            ,apd.STREET_NUMBER
            ,apd.STREET_SUFFIX
            ,apd.STREET_DIR_PREFIX
            ,apd.STREET_DIR_SUFFIX
            ,apd.UNIT_NUMBER
            ,PRIMARY_LISTING.VALUE:listDate
            ,apd.BUILDING_NAME
            ,apd.BUILDERS
            ,apd.LATITUDE
            ,apd.LONGITUDE
            ,iml.ISMEMBERLISTING
            ,ROW_NUMBER() OVER (PARTITION BY apd.STATE_ID ORDER BY PRIMARY_LISTING.VALUE:listDate DESC) AS PROPERTY_COUNT
        FROM
            SITEMAP.RESI_SILVER.ACTIVE_PROPERTY_DETAIL apd
            INNER JOIN IS_MEMBER_LISTING_DATA iml ON iml.PROPERTY_ID = apd.PROPERTY_ID
            ,LATERAL FLATTEN(input => apd.PRIMARY_LISTINGS) PRIMARY_LISTING
        WHERE 1 = 1
            AND PRIMARY_LISTING.VALUE:isForSale = true
            AND PRIMARY_LISTING.VALUE:isForRent IS NULL
            AND STATE_ID != 254                                 // Exclude Puerto Rico
            AND STATE_ID IS NOT NULL
    )

    SELECT
        STATE_ID::STRING AS OBJECTKEY
        ,ARRAY_AGG(OBJECT_CONSTRUCT(
            'propertyId', PROPERTY_ID
            ,'city', CITY
            ,'state', STATE
            ,'zipCode', ZIP_CODE
            ,'fullStreetAddress', FULL_STREET_ADDRESS
            ,'streetName', STREET_NAME
            ,'streetNumber', STREET_NUMBER
            ,'streetSuffix', STREET_SUFFIX
            ,'streetDirPrefix', STREET_DIR_PREFIX
            ,'streetDirSuffix', STREET_DIR_SUFFIX
            ,'unitNumber', UNIT_NUMBER
            ,'buildingName', BUILDING_NAME
            ,'builders', BUILDERS
            ,'latitude', LATITUDE
            ,'longitude', LONGITUDE
            ,'isMemberListing', ISMEMBERLISTING
            ,'primaryListings', PRIMARY_LISTINGS)) AS JSONDATA
        ,COUNT(1) AS OBJECTCOUNT
    FROM
        FORSALE_ACTIVE_PROPERTY_DETAIL
    WHERE 
        PROPERTY_COUNT <= 1000
    GROUP BY
        STATE_ID
    ORDER BY
        STATE_ID
    ;