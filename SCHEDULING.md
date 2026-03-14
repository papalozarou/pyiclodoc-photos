# Scheduling

## Modes

- `interval`: run every `SCHEDULE_INTERVAL_MINUTES`.
- `daily`: run once per day at `SCHEDULE_BACKUP_TIME`.
- `weekly`: run once per week on `SCHEDULE_WEEKDAYS`.
- `twice_weekly`: run twice per week on two comma-separated weekdays.
- `monthly`: run on the configured weekday in the configured week of the month.
- `RUN_ONCE=true`: run once and exit. The configured schedule is ignored.

## Examples

- Daily at 02:00:
  `SCHEDULE_MODE=daily`
  `SCHEDULE_BACKUP_TIME=02:00`
- Weekly on Monday at 21:30:
  `SCHEDULE_MODE=weekly`
  `SCHEDULE_WEEKDAYS=monday`
  `SCHEDULE_BACKUP_TIME=21:30`
- Twice weekly on Monday and Thursday at 03:15:
  `SCHEDULE_MODE=twice_weekly`
  `SCHEDULE_WEEKDAYS=monday,thursday`
  `SCHEDULE_BACKUP_TIME=03:15`
- Monthly on the last Sunday at 05:00:
  `SCHEDULE_MODE=monthly`
  `SCHEDULE_WEEKDAYS=sunday`
  `SCHEDULE_MONTHLY_WEEK=last`
  `SCHEDULE_BACKUP_TIME=05:00`

## Manual backup

Send this Telegram command from the configured chat:

```text
<username> backup
```

That triggers an immediate backup and the configured schedule remains in place afterwards.
