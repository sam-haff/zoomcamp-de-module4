1. **>Understanding dbt model resolution** \
   **Answer:** select * from myproject.my_nyc_tripdata.ext_green_taxi

   Explanation:
    - Intuition: resolved name should match BigQuery native table identifier. But you also can inspect compiled code for the model inside DBT Cloud IDE.
2. **dbt Variables & Dynamic Models** \
   **Answer:** Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY
   
   Explanation:
    - That way we call <em>var</em> function which extracts variable value(from args) if available, but has <em>env_var</em> as fallback(default) value. So if we don't have variable set from args, we return the environment variable. If it's also unavailable, we resort to a simple default value "30".
3. **dbt Data Lineage and Execution**\
   **Answer:** dbt run --select models/staging/+

   Explanation:
    - dim_zone_lookup is not built in that setting but is required for the upstream models.
4. **dbt Macros and Jinja** \
   **Answer:**
    - Setting a value for DBT_BIGQUERY_TARGET_DATASET env var is mandatory, or it'll fail to compile
    - When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET
    - When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET
    - When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET
    
    Explanation:
    - Since no default is available, code with <em>env_var</em> fails to compile(there is no way to decide which value it should have).\
    Both <em>stg</em> and <em>staging</em> behave the same way, because the condition is != "core", which both values obey.

5. **Taxi Quarterly Revenue Growth**\
   **Answer:** green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}

   Explanation:
    - After we created and built necessary models(models/core/fact_quaterly_revenue), we can use the following queries to get the answer:
        ~~~
        -- best yellow quater
        SELECT year, quater
        FROM `kestra-de.<dbt_dataset>.fact_quaterly_revenue`
        WHERE service_type = 'Yellow' and year=2020
        ORDER BY yoy_growth DESC
        LIMIT 1;
        -- worst
        SELECT year, quater
        FROM `kestra-de.<dbt_dataset>.fact_quaterly_revenue`
        WHERE service_type = 'Yellow' and year=2020
        ORDER BY yoy_growth
        LIMIT 1;
        ~~~
        Similar for the Green taxi.
6. **P97/P95/P90 Taxi Monthly Fare**\
   **Answer:** green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

   Explanation: 
    - After we created and built necessary models(<em>models/core/fact_monthly_fare_p95</em>), we can use the following queries to get the answer:
        ~~~
        SELECT *
        FROM `kestra-de.<dbt_dataset>.fact_monthly_fare_p95`
        WHERE month=4 AND year=2020
        ORDER BY service_type
        LIMIT 2;
        ~~~
7. **Top #Nth longest P90 travel time Location for FHV**
   **Answer:** LaGuardia Airport, Chinatown, Garment District

   Explanation:
    - After we created and built necessary models:
      - <em>models/core/fact_fhv_monthly_zone_traveltime_p90</em>
      - <em>models/core/dim_fhv_trips</em>
      - <em>models/staging/stg_fhv_trips</em>
  
      We can use the following query to get the answer:
        ~~~
        SELECT * FROM `kestra-de.<dbt_dataset>.fact_fhv_monthly_zone_traveltime_p90` 
        WHERE pickup_zone="Newark Airport" and month=11 and year=2019
        ORDER BY p90 DESC
        LIMIT 3;
        ~~~
        Similar for <em>SoHo</em> and <em>Yorkville East</em>












   
   