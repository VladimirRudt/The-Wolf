//! The `Trace` object: the stream-wide header with the clock that event timestamps use.

use crate::cursor::{ByteCursor, Malformed};

/// Stream-wide facts needed to interpret events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceInfo {
    /// UTC time at `sync_qpc`, in ns since the Unix epoch. The runtime records it with
    /// millisecond precision; durations between events keep full precision.
    pub sync_time_utc_ns: i64,
    /// Clock value (QPC ticks) at `sync_time_utc_ns`.
    pub sync_qpc: i64,
    /// QPC ticks per second (10 MHz on Windows, 1 GHz on Linux).
    pub qpc_frequency: i64,
    /// Size of a pointer in the target process (4 or 8), for payloads that contain pointers.
    pub pointer_size: u32,
}

impl TraceInfo {
    /// Size of the `Trace` object's fields.
    pub(crate) const SIZE: usize = 48;

    pub(crate) fn parse(cursor: &mut ByteCursor<'_>) -> Result<Self, Malformed> {
        // SyncTimeUTC is a Windows SYSTEMTIME: eight u16 fields.
        let year = cursor.u16()?;
        let month = cursor.u16()?;
        let _day_of_week = cursor.u16()?;
        let day = cursor.u16()?;
        let hour = cursor.u16()?;
        let minute = cursor.u16()?;
        let second = cursor.u16()?;
        let millisecond = cursor.u16()?;
        let sync_qpc = cursor.i64()?;
        let qpc_frequency = cursor.i64()?;
        let pointer_size = cursor.u32()?;
        let _process_id = cursor.u32()?;
        let _number_of_processors = cursor.u32()?;
        let _expected_cpu_sampling_rate = cursor.u32()?;

        if !(1..=12).contains(&month)
            || !(1..=31).contains(&day)
            || hour > 23
            || minute > 59
            || second > 60
            || millisecond > 999
        {
            return Err(Malformed("invalid sync time"));
        }
        if qpc_frequency <= 0 {
            return Err(Malformed("invalid QPC frequency"));
        }
        let seconds = days_from_civil(i64::from(year), i64::from(month), i64::from(day)) * 86_400
            + i64::from(hour) * 3_600
            + i64::from(minute) * 60
            + i64::from(second);
        // Years outside ~1678..2262 don't fit in i64 nanoseconds.
        let sync_time_utc_ns = seconds
            .checked_mul(1_000_000_000)
            .and_then(|ns| ns.checked_add(i64::from(millisecond) * 1_000_000))
            .ok_or(Malformed("sync time out of range"))?;
        Ok(Self {
            sync_time_utc_ns,
            sync_qpc,
            qpc_frequency,
            pointer_size,
        })
    }

    /// Converts an event timestamp (QPC ticks) to UTC ns since the Unix epoch.
    pub fn to_utc_ns(&self, qpc: i64) -> i64 {
        let elapsed_ns = (i128::from(qpc) - i128::from(self.sync_qpc)) * 1_000_000_000
            / i128::from(self.qpc_frequency);
        let utc_ns = i128::from(self.sync_time_utc_ns) + elapsed_ns;
        i64::try_from(utc_ns).unwrap_or(if utc_ns < 0 { i64::MIN } else { i64::MAX })
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_from_march = (month + 9) % 12;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// `Trace` fields: 2025-10-19 03:14:05.250 UTC at QPC 1000, 10 MHz, 64-bit.
    pub(crate) fn trace_fields() -> Vec<u8> {
        let mut bytes = Vec::new();
        for field in [2025u16, 10, 0, 19, 3, 14, 5, 250] {
            bytes.extend_from_slice(&field.to_le_bytes());
        }
        bytes.extend_from_slice(&1000i64.to_le_bytes());
        bytes.extend_from_slice(&10_000_000i64.to_le_bytes());
        for field in [8u32, 4812, 16, 0] {
            bytes.extend_from_slice(&field.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn days_from_civil_matches_known_dates() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(days_from_civil(2025, 10, 19), 20_380);
    }

    #[test]
    fn parses_trace_fields_and_converts_timestamps() {
        let bytes = trace_fields();
        assert_eq!(bytes.len(), TraceInfo::SIZE);
        let info = TraceInfo::parse(&mut ByteCursor::new(&bytes)).expect("parse");
        // 2025-10-19T03:14:05.250Z
        assert_eq!(info.sync_time_utc_ns, 1_760_843_645_250_000_000);
        assert_eq!(info.pointer_size, 8);
        assert_eq!(info.to_utc_ns(1000), info.sync_time_utc_ns);
        // 25 ticks of 100 ns later.
        assert_eq!(info.to_utc_ns(1025), info.sync_time_utc_ns + 2_500);
        assert_eq!(info.to_utc_ns(975), info.sync_time_utc_ns - 2_500);
    }

    #[test]
    fn rejects_a_year_past_the_i64_range() {
        let mut bytes = trace_fields();
        bytes[0..2].copy_from_slice(&u16::MAX.to_le_bytes());
        assert_eq!(
            TraceInfo::parse(&mut ByteCursor::new(&bytes)),
            Err(Malformed("sync time out of range"))
        );
    }

    #[test]
    fn rejects_zero_frequency() {
        let mut bytes = trace_fields();
        bytes[24..32].copy_from_slice(&0i64.to_le_bytes());
        assert!(TraceInfo::parse(&mut ByteCursor::new(&bytes)).is_err());
    }
}
