{#
Returns quater based on the date
#}

{% macro get_date_quater(i_date) -%}
    cast((cast(EXTRACT(MONTH FROM {{i_date}}) as integer)-1)/4 as integer) + 1
{%-endmacro%}