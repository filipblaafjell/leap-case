## Leap Case Analytics (dbt project)

Focused dbt project modeling Amazon purchase & survey data enriched with FRED macro + state unemployment indicators. Layered approach: clean → aggregate/enrich → business marts.

### Data Sources
- Amazon purchases (transactional events)
- Amazon survey (user demographics & behavior attributes)
- FRED macro (national monthly indicators)
- FRED unemployment (state monthly rates)

### Layer Overview
| Layer | Purpose | Grain Examples |
|-------|---------|----------------|
| staging | Type casting, null & text normalization, state validation | purchase event, user, month, state-month |
| intermediate | Aggregations + demographic/economic context | user-month, user-state-month, product-state-month, national month |
| marts | Business-facing metrics & growth calculations | state performance, demographics, product trends, macro correlation |

### Key Conventions
- Dates standardized with `date_trunc('month')`
- Valid US states only via `filter_valid_states` macro
- Growth metrics calculated only in marts
- Core tests: uniqueness, not_null, domain plausibility (positive prices, unemployment 0–100)

### Running
```
dbt deps   # install packages
dbt run    # build models
dbt test   # execute data tests
dbt docs generate && dbt docs serve  # optional documentation site
```

### Common Model Entry Points
- `stg_amazon_purchases`, `stg_amazon_survey`
- `int_user_state_behavior`, `int_product_state_month`
- `mart_state_performance`, `mart_state_product_trends`

