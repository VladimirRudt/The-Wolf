//! A cursor over one in-memory slice: bounds-checked little-endian reads (integers, varints,
//! UTF-16 strings). Knows nothing about nettrace; the block and stream code build on it.

/// Contents that don't match the format. Inside a block this skips the block; in the stream
/// framing it becomes [`ParseError::Malformed`](crate::ParseError::Malformed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Malformed(pub(crate) &'static str);

#[derive(Debug, Clone)]
pub(crate) struct ByteCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> ByteCursor<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub(crate) fn bytes(&mut self, n: usize) -> Result<&'a [u8], Malformed> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&end| end <= self.data.len())
            .ok_or(Malformed("truncated"))?;
        let bytes = &self.data[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], Malformed> {
        let mut array = [0; N];
        array.copy_from_slice(self.bytes(N)?);
        Ok(array)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, Malformed> {
        Ok(self.array::<1>()?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, Malformed> {
        self.array().map(u16::from_le_bytes)
    }

    pub(crate) fn u32(&mut self) -> Result<u32, Malformed> {
        self.array().map(u32::from_le_bytes)
    }

    pub(crate) fn u64(&mut self) -> Result<u64, Malformed> {
        self.array().map(u64::from_le_bytes)
    }

    pub(crate) fn i64(&mut self) -> Result<i64, Malformed> {
        self.array().map(i64::from_le_bytes)
    }

    /// 7 bits per byte, least significant first; the high bit says another byte follows.
    fn varint(&mut self, max_bytes: u32) -> Result<u64, Malformed> {
        let mut value = 0u64;
        for i in 0..max_bytes {
            let byte = self.u8()?;
            let bits = u64::from(byte & 0x7F);
            // The 10th byte has room for 1 bit; anything more would be lost.
            if bits.leading_zeros() < 7 * i {
                return Err(Malformed("varint64 overflow"));
            }
            value |= bits << (7 * i);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(Malformed("varint too long"))
    }

    pub(crate) fn varint_u32(&mut self) -> Result<u32, Malformed> {
        u32::try_from(self.varint(5)?).map_err(|_| Malformed("varint32 overflow"))
    }

    pub(crate) fn varint_u64(&mut self) -> Result<u64, Malformed> {
        self.varint(10)
    }

    /// UTF-16 string terminated by a NUL code unit.
    pub(crate) fn utf16z(&mut self) -> Result<String, Malformed> {
        let mut units = Vec::new();
        loop {
            match self.u16()? {
                0 => return Ok(String::from_utf16_lossy(&units)),
                unit => units.push(unit),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_little_endian_numbers() {
        let mut cursor = ByteCursor::new(&[1, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(cursor.u16(), Ok(1));
        assert_eq!(cursor.u32(), Ok(2));
        assert_eq!(cursor.u64(), Ok(3));
        assert_eq!(cursor.remaining(), 0);
        assert_eq!(cursor.u8(), Err(Malformed("truncated")));
    }

    #[test]
    fn reads_varints() {
        let mut cursor = ByteCursor::new(&[0x05, 0xAC, 0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_eq!(cursor.varint_u32(), Ok(5));
        assert_eq!(cursor.varint_u32(), Ok(300));
        assert_eq!(cursor.varint_u32(), Ok(u32::MAX));
    }

    #[test]
    fn rejects_overlong_and_overflowing_varints() {
        assert!(ByteCursor::new(&[0x80; 6]).varint_u32().is_err());
        assert!(
            ByteCursor::new(&[0xFF, 0xFF, 0xFF, 0xFF, 0x1F])
                .varint_u32()
                .is_err()
        );
        assert!(ByteCursor::new(&[0x80; 11]).varint_u64().is_err());

        let mut max = [0xFF; 10];
        max[9] = 0x01;
        assert_eq!(ByteCursor::new(&max).varint_u64(), Ok(u64::MAX));
        max[9] = 0x02;
        assert_eq!(
            ByteCursor::new(&max).varint_u64(),
            Err(Malformed("varint64 overflow"))
        );
    }

    #[test]
    fn reads_utf16_strings() {
        let mut cursor = ByteCursor::new(&[b'G', 0, b'C', 0, 0, 0, 0xFF]);
        assert_eq!(cursor.utf16z().as_deref(), Ok("GC"));
        assert_eq!(cursor.remaining(), 1);
        assert!(ByteCursor::new(&[b'G', 0]).utf16z().is_err());
    }
}
