# Embedder comparison — 45 catalog entries, 24 questions (8 paraphrases)

| Embedder | hit@1 | hit@3 | MRR | hit@1 within kind | paraphrase hit@1 | paraphrase hit@3 | paraphrase MRR | query embed p50 | max |
|---|---|---|---|---|---|---|---|---|---|
| `hash-v1-384` | 46% | 58% | 0.56 | 79% | 38% | 50% | 0.48 | 0.0 ms | 0.0 ms |
| `sentence-transformers/all-MiniLM-L6-v2` | 58% | 83% | 0.73 | 92% | 88% | 88% | 0.90 | 5.9 ms | 16.9 ms |

## Misses at rank 1 — `hash-v1-384`

- "how far down the funnel do visitors get on page 1" → expected `template:funnel_by_page_type` at rank 10; ranked `column:hourly_traffic.avg_funnel_depth` first
- "busiest hours of the day last week" → expected `template:hourly_traffic` at rank 2; ranked `template:daily_conversion_trend` first
- "traffic by hour last week" → expected `template:hourly_traffic` at rank 10; ranked `column:hourly_traffic.hour` first
- "week over week trend in conversions" → expected `template:daily_conversion_trend` at rank 7; ranked `column:hourly_traffic.hour` first
- "table with funnel conversion analysis by page type" → expected `table:funnel_analysis` at rank 7; ranked `column:funnel_analysis.page_type` first
- "hourly traffic patterns table" → expected `table:hourly_traffic` at rank 9; ranked `column:hourly_traffic.hour` first
- "user level engagement metrics table" → expected `table:user_engagement` at rank 17; ranked `column:user_engagement.user_id` first
- "summary statistics per page type" → expected `table:page_type_summary` at rank 17; ranked `column:page_type_summary.page_type` first
- "average session duration in seconds" → expected `column:page_type_summary.avg_duration_seconds` at rank 2; ranked `template:page_type_summary` first
- "how long do sessions last" → expected `column:page_type_summary.avg_duration_seconds` at rank 24; ranked `column:hourly_traffic.unique_users` first
- "when was each user first seen" → expected `column:user_engagement.first_seen` at rank 2; ranked `template:top_engaged_users` first
- "percentage of impressions reaching stage f" → expected `column:page_type_summary.pct_reaching_f` at rank 15; ranked `table:page_type_summary` first
- "number of distinct users per hour" → expected `column:hourly_traffic.unique_users` at rank 15; ranked `column:page_type_summary.pct_reaching_f` first

## Misses at rank 1 — `sentence-transformers/all-MiniLM-L6-v2`

- "headline numbers per page type" → expected `template:page_type_summary` at rank 2; ranked `column:page_type_summary.page_type` first
- "how engaged are users on each page type" → expected `template:page_type_summary` at rank 6; ranked `column:user_engagement.most_engaged_page_type` first
- "who are the most engaged users" → expected `template:top_engaged_users` at rank 2; ranked `column:user_engagement.most_engaged_page_type` first
- "impression volume by hour for a date range" → expected `template:hourly_traffic` at rank 2; ranked `column:hourly_traffic.hour` first
- "traffic by hour last week" → expected `template:hourly_traffic` at rank 3; ranked `column:hourly_traffic.event_date` first
- "table with funnel conversion analysis by page type" → expected `table:funnel_analysis` at rank 2; ranked `column:funnel_analysis.page_type` first
- "hourly traffic patterns table" → expected `table:hourly_traffic` at rank 5; ranked `column:hourly_traffic.hour` first
- "summary statistics per page type" → expected `table:page_type_summary` at rank 2; ranked `column:page_type_summary.page_type` first
- "how long do sessions last" → expected `column:page_type_summary.avg_duration_seconds` at rank 5; ranked `column:user_engagement.last_seen` first
- "percentage of impressions reaching stage f" → expected `column:page_type_summary.pct_reaching_f` at rank 35; ranked `template:funnel_by_page_type` first
