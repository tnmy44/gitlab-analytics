SELECT
    json_extract_path_text(payload, 'costs.total')::FLOAT AS costs_total,
    json_extract_path_text(payload, 'costs.dimensions')::ARRAY AS costs_dimensions,
    json_extract_path_text(payload, 'trials')::FLOAT AS trials, 
    json_extract_path_text(payload, 'hourly_rate') AS hourly_rate,
    json_extract_path_text(payload, 'balance.available')::FLOAT AS balance_available,
    json_extract_path_text(payload, 'balance.remaining')::FLOAT AS balance_remaining,
    json_extract_path_text(payload, 'balance.line_items')::ARRAY AS balance_line_items
FROM {{ source('elastic_billing', 'costs_overview') }}