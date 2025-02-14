{#
Returns description of the payment type 
#}

{% macro get_payment_type_desc(type) -%}
    case cast({{type}} as integer)
        when 1 then "Credit card"
        when 2 then "Cash"
        when 3 then "No charge"
        when 4 then "Dispute"
        when 5 then "Uknown"
        when 6 then "Voided trip"
        else "Empty"
    end
{%-endmacro%}