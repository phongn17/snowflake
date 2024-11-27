CREATE OR REPLACE PROCEDURE SITEMAP.PUBLIC.CREATE_FAST_HTML_SITEMAP("SOURCE_TABLE" VARCHAR(16777216), "DOMAIN" VARCHAR(16777216), , "STRUCTURED_PATH_LENGTH" FLOAT)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT='user-defined procedure'
EXECUTE AS CALLER
AS 'BEGIN

  LET stmt STRING := ''COPY INTO '' || ''0'' || DOMAIN || ''_GOLD.SITEMAP_EXPORT_STAGE/sitemap-stages/html-sitemap'' 
    || $$ FROM (SELECT OBJECT_CONSTRUCT(''Key'', ObjectKey,
                                    ''Data'', JsonData,
                                    ''SitemapName'', ''$$ || SITEMAP_NAME || $$'',
                                    ''DestinationPath'', ''shared/export/homes-sitemap-export/ht,l-sitemaps'',
                                    ''FileExtension'', ''json.gz'',
                                    ''StructurePathLength'', $$ || STRUCTURED_PATH_LENGTH || $$,
                                    ''StorageType'', $$ || STORAGE_TYPE || $$) -- 1 for S3, 2 for Redis, 3 for both
                    FROM IDENTIFIER()
                )
        FILE_FORMAT = [
            TYPE = ''JSON''
            COMPRESSION = ''GZIP''
        ]
        OVERWRITE = TRUE
        SINGLE = FALSE;
    $$;

    EXECUTE IMMEDIATE :stmt USING(SOURCE_TABLE);

    RETURN ''HTML sitemaps generated successfully.'';
END;
';