use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{IpcError, ServerError};

/// Magic of protocol version 1: `DOTNET_IPC_V1` plus a terminating NUL.
pub const PROTOCOL_MAGIC: &[u8; 14] = b"DOTNET_IPC_V1\0";

/// magic (14) + size (2) + command set (1) + command id (1) + reserved (2).
const HEADER_SIZE: usize = 20;

/// Command set and id: together they name a command, or a server response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommandCode {
    pub set: u8,
    pub id: u8,
}

impl CommandCode {
    pub const PROCESS_INFO: Self = Self::new(0x04, 0x00);
    pub const PROCESS_INFO2: Self = Self::new(0x04, 0x04);
    pub const PROCESS_INFO3: Self = Self::new(0x04, 0x08);
    pub const SERVER_OK: Self = Self::new(0xFF, 0x00);
    pub const SERVER_ERROR: Self = Self::new(0xFF, 0xFF);

    pub const fn new(set: u8, id: u8) -> Self {
        Self { set, id }
    }
}

/// One Diagnostic IPC message: header plus payload. Numbers are little-endian.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub command: CommandCode,
    pub payload: Vec<u8>,
}

impl Message {
    /// Wire bytes of a message.
    pub fn encode(command: CommandCode, payload: &[u8]) -> Result<Vec<u8>, IpcError> {
        let size = u16::try_from(HEADER_SIZE + payload.len())
            .map_err(|_| IpcError::PayloadTooLarge(payload.len()))?;
        let mut bytes = Vec::with_capacity(usize::from(size));
        bytes.extend_from_slice(PROTOCOL_MAGIC);
        bytes.extend_from_slice(&size.to_le_bytes());
        bytes.push(command.set);
        bytes.push(command.id);
        bytes.extend_from_slice(&0u16.to_le_bytes()); // reserved, must be 0
        bytes.extend_from_slice(payload);
        Ok(bytes)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, IpcError> {
        Self::encode(self.command, &self.payload)
    }

    pub async fn write<S: AsyncWrite + Unpin>(
        stream: &mut S,
        command: CommandCode,
        payload: &[u8],
    ) -> Result<(), IpcError> {
        stream.write_all(&Self::encode(command, payload)?).await?;
        stream.flush().await?;
        Ok(())
    }

    pub async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Self, IpcError> {
        let mut header = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header).await?;
        let (magic, rest) = header.split_at(PROTOCOL_MAGIC.len());
        if magic != PROTOCOL_MAGIC {
            return Err(IpcError::BadMagic);
        }
        let size = usize::from(u16::from_le_bytes([rest[0], rest[1]]));
        let payload_len = size.checked_sub(HEADER_SIZE).ok_or(IpcError::Malformed(
            "message size is smaller than the header",
        ))?;
        let mut payload = vec![0; payload_len];
        stream.read_exact(&mut payload).await?;
        Ok(Self {
            command: CommandCode::new(rest[2], rest[3]),
            payload,
        })
    }

    /// The payload of an OK response; an error response becomes [`IpcError::Server`].
    pub fn into_ok_payload(self) -> Result<Vec<u8>, IpcError> {
        match self.command {
            CommandCode::SERVER_OK => Ok(self.payload),
            CommandCode::SERVER_ERROR => {
                let code = PayloadReader::new(&self.payload)
                    .u32()
                    .map_err(|_| IpcError::Malformed("error response without an error code"))?;
                Err(IpcError::Server(ServerError(code)))
            }
            other => Err(IpcError::UnexpectedResponse(other)),
        }
    }
}

/// Bounds-checked reader for payload fields. Truncated input is an error, never a panic.
pub(crate) struct PayloadReader<'a> {
    bytes: &'a [u8],
}

impl<'a> PayloadReader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], IpcError> {
        if len > self.bytes.len() {
            return Err(IpcError::Malformed("payload is truncated"));
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], IpcError> {
        let mut array = [0; N];
        array.copy_from_slice(self.take(N)?);
        Ok(array)
    }

    pub(crate) fn u32(&mut self) -> Result<u32, IpcError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, IpcError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    /// `string` = u32 length in UTF-16 units (including the terminating NUL), then the units.
    /// Length 0 is the empty string. Invalid UTF-16 is replaced, not rejected.
    pub(crate) fn string(&mut self) -> Result<String, IpcError> {
        let len = self.u32()? as usize;
        let byte_len = len
            .checked_mul(2)
            .ok_or(IpcError::Malformed("string length overflows"))?;
        let units: Vec<u16> = self
            .take(byte_len)?
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        let units = units.strip_suffix(&[0]).unwrap_or(&units);
        Ok(String::from_utf16_lossy(units))
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    /// Builds payloads in the protocol's encoding.
    #[derive(Default)]
    pub(crate) struct PayloadBuilder(Vec<u8>);

    impl PayloadBuilder {
        pub(crate) fn u32(mut self, value: u32) -> Self {
            self.0.extend_from_slice(&value.to_le_bytes());
            self
        }

        pub(crate) fn u64(mut self, value: u64) -> Self {
            self.0.extend_from_slice(&value.to_le_bytes());
            self
        }

        pub(crate) fn bytes(mut self, bytes: &[u8]) -> Self {
            self.0.extend_from_slice(bytes);
            self
        }

        pub(crate) fn string(mut self, value: &str) -> Self {
            let mut units: Vec<u16> = value.encode_utf16().collect();
            units.push(0);
            self = self.u32(units.len() as u32);
            for unit in units {
                self.0.extend_from_slice(&unit.to_le_bytes());
            }
            self
        }

        pub(crate) fn build(self) -> Vec<u8> {
            self.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::PayloadBuilder;
    use super::*;

    #[test]
    fn encodes_header_per_spec() {
        let bytes = Message::encode(CommandCode::PROCESS_INFO, &[]).expect("encode");
        let mut expected = PROTOCOL_MAGIC.to_vec();
        expected.extend_from_slice(&[20, 0, 0x04, 0x00, 0, 0]);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn rejects_oversized_payload() {
        let payload = vec![0; usize::from(u16::MAX)];
        assert!(matches!(
            Message::encode(CommandCode::PROCESS_INFO, &payload),
            Err(IpcError::PayloadTooLarge(_))
        ));
    }

    #[tokio::test]
    async fn reads_what_it_writes() {
        let mut buffer = Vec::new();
        Message::write(&mut buffer, CommandCode::SERVER_OK, &[1, 2, 3])
            .await
            .expect("write");
        let message = Message::read(&mut &buffer[..]).await.expect("read");
        assert_eq!(message.command, CommandCode::SERVER_OK);
        assert_eq!(message.into_ok_payload().expect("ok"), [1, 2, 3]);
    }

    #[tokio::test]
    async fn rejects_bad_magic() {
        let mut bytes = Message::encode(CommandCode::SERVER_OK, &[]).expect("encode");
        bytes[0] = b'X';
        assert!(matches!(
            Message::read(&mut &bytes[..]).await,
            Err(IpcError::BadMagic)
        ));
    }

    #[tokio::test]
    async fn rejects_size_smaller_than_header() {
        let mut bytes = Message::encode(CommandCode::SERVER_OK, &[]).expect("encode");
        bytes[14] = 19;
        assert!(matches!(
            Message::read(&mut &bytes[..]).await,
            Err(IpcError::Malformed(_))
        ));
    }

    #[tokio::test]
    async fn truncated_stream_is_an_io_error() {
        let bytes = Message::encode(CommandCode::SERVER_OK, &[1, 2, 3]).expect("encode");
        let truncated = &bytes[..bytes.len() - 1];
        assert!(matches!(
            Message::read(&mut &truncated[..]).await,
            Err(IpcError::Io(_))
        ));
    }

    #[test]
    fn error_response_carries_hresult() {
        let message = Message {
            command: CommandCode::SERVER_ERROR,
            payload: PayloadBuilder::default().u32(0x8013_1385).build(),
        };
        assert!(matches!(
            message.into_ok_payload(),
            Err(IpcError::Server(ServerError::UNKNOWN_COMMAND))
        ));
    }

    #[test]
    fn unexpected_response_is_reported() {
        let message = Message {
            command: CommandCode::PROCESS_INFO,
            payload: Vec::new(),
        };
        assert!(matches!(
            message.into_ok_payload(),
            Err(IpcError::UnexpectedResponse(CommandCode::PROCESS_INFO))
        ));
    }

    #[test]
    fn reads_strings() {
        let payload = PayloadBuilder::default()
            .string("héllo")
            .u32(0) // empty string encoded as length 0
            .build();
        let mut reader = PayloadReader::new(&payload);
        assert_eq!(reader.string().expect("string"), "héllo");
        assert_eq!(reader.string().expect("empty"), "");
    }

    #[test]
    fn string_longer_than_payload_is_malformed() {
        let payload = PayloadBuilder::default().u32(u32::MAX).build();
        assert!(matches!(
            PayloadReader::new(&payload).string(),
            Err(IpcError::Malformed(_))
        ));
    }
}
