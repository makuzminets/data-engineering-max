{% macro calculate_risk_score(
    has_fraud_history,
    uses_emulator,
    is_rooted,
    multi_country,
    multi_device,
    is_new_account
) %}
{#
    Calculate user risk score based on risk signals.
    Returns a score from 0 to 100.
    
    Usage:
    {{ calculate_risk_score(
        has_fraud_history='has_fraud_history',
        uses_emulator='uses_emulator',
        is_rooted='is_rooted',
        multi_country='multi_country_user',
        multi_device='multi_device_user',
        is_new_account='is_new_account'
    ) }}
#}
(
    case when {{ has_fraud_history }} then 30 else 0 end +
    case when {{ uses_emulator }} then 20 else 0 end +
    case when {{ is_rooted }} then 15 else 0 end +
    case when {{ multi_country }} then 10 else 0 end +
    case when {{ multi_device }} then 10 else 0 end +
    case when {{ is_new_account }} then 15 else 0 end
)
{% endmacro %}


{% macro get_risk_tier(risk_score) %}
{#
    Convert risk score to tier.
    
    Usage:
    {{ get_risk_tier('risk_score') }} as risk_tier
#}
case
    when {{ risk_score }} >= 50 then 'high'
    when {{ risk_score }} >= 25 then 'medium'
    else 'low'
end
{% endmacro %}
