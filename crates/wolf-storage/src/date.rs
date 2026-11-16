//! UTC calendar dates for segment file names (`seg-YYYY-MM-DD.duckdb`). Only what the
//! segmented store needs: nanos → day, day → `YYYY-MM-DD`, and back. Hand-rolled
//! (Howard Hinnant's civil-from-days algorithms) to avoid a chrono dependency.

use std::fmt;

/// Nanoseconds per day; segment boundaries are UTC midnight.
const DAY_NS: i64 = 86_400_000_000_000;

/// A UTC calendar day, as days since 1970-01-01.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Date(i64);

impl Date {
    pub(crate) fn from_nanos(ns: i64) -> Self {
        Self(ns.div_euclid(DAY_NS))
    }

    /// Nanosecond timestamp of this day's UTC midnight.
    pub(crate) fn start_nanos(self) -> i64 {
        self.0 * DAY_NS
    }

    /// Parses `YYYY-MM-DD` (the segment file name body).
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let (y, rest) = text.split_once('-')?;
        let (m, d) = rest.split_once('-')?;
        let year: i64 = y.parse().ok()?;
        let month: i64 = m.parse().ok()?;
        let day: i64 = d.parse().ok()?;
        if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return None;
        }
        // days_from_civil (Hinnant).
        let y = if month <= 2 { year - 1 } else { year };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = y - era * 400;
        let mp = if month > 2 { month - 3 } else { month + 9 };
        let doy = (153 * mp + 2) / 5 + day - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        Some(Self(era * 146_097 + doe - 719_468))
    }
}

impl fmt::Display for Date {
    /// civil_from_days (Hinnant), zero-padded `YYYY-MM-DD`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let z = self.0 + 719_468;
        let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = yoe + era * 400 + i64::from(month <= 2);
        write!(f, "{year:04}-{month:02}-{day:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_dates() {
        for text in ["1970-01-01", "2024-02-29", "2026-05-20", "1999-12-31"] {
            let date = Date::parse(text).expect("parse");
            assert_eq!(date.to_string(), text);
        }
    }

    #[test]
    fn from_nanos_floors_to_utc_midnight() {
        let day = Date::parse("2026-05-20").expect("date");
        // Any time inside the day maps to the same date; negative nanos stay correct.
        assert_eq!(Date::from_nanos(day.0 * DAY_NS), day);
        assert_eq!(Date::from_nanos(day.0 * DAY_NS + DAY_NS - 1), day);
        assert_eq!(
            Date::from_nanos(-1),
            Date::parse("1969-12-31").expect("epoch-1")
        );
    }

    #[test]
    fn rejects_garbage() {
        assert!(Date::parse("2026-13-01").is_none());
        assert!(Date::parse("not-a-date").is_none());
        assert!(Date::parse("2026-05").is_none());
    }
}
