use crate::error::SatelError;
use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit, generic_array::GenericArray};
use aes::Aes192;
use bytes::{Buf, BytesMut};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[allow(dead_code)]
pub const AES_BLOCK_SIZE: usize = 16;
pub const AES_KEY_SIZE: usize = 24; // AES-192

/// Converts a Satel integration key (up to 12 ASCII characters) into a 24-byte AES-192 key.
///
/// Algorithm:
/// 1. The key characters are padded with ASCII space (`0x20`) up to 12 bytes.
/// 2. The 12-byte block is duplicated: `key[0..12] == key[12..24]`.
pub fn derive_aes_key(integration_key: &str) -> [u8; AES_KEY_SIZE] {
    let key_bytes = integration_key.as_bytes();
    let mut key = [0x20u8; AES_KEY_SIZE];

    for i in 0..12 {
        let b = if i < key_bytes.len() {
            key_bytes[i]
        } else {
            0x20
        };
        key[i] = b;
        key[i + 12] = b;
    }

    key
}

/// Helper performing in-place Satel ETHM-1 AES-192 encryption and decryption.
#[derive(Clone)]
pub struct SatelAesCipher {
    cipher: Aes192,
}

impl SatelAesCipher {
    pub fn new(key: [u8; AES_KEY_SIZE]) -> Self {
        Self {
            cipher: Aes192::new(GenericArray::from_slice(&key)),
        }
    }

    /// Encrypts given buffer of bytes in place according to Satel ETHM-1 CBC/stream cipher specification.
    pub fn encrypt(&self, buffer: &mut [u8]) {
        let mut cv = [0u8; 16];
        self.cipher.encrypt_block(GenericArray::from_mut_slice(&mut cv));

        let mut index = 0;
        let mut count = buffer.len();

        while count > 0 {
            if count > 15 {
                count -= 16;
                let mut p = [0u8; 16];
                p.copy_from_slice(&buffer[index..index + 16]);
                for i in 0..16 {
                    p[i] ^= cv[i];
                }
                self.cipher.encrypt_block(GenericArray::from_mut_slice(&mut p));
                cv.copy_from_slice(&p);
                buffer[index..index + 16].copy_from_slice(&p);
                index += 16;
            } else {
                let mut p = [0u8; 16];
                p[..count].copy_from_slice(&buffer[index..index + count]);
                self.cipher.encrypt_block(GenericArray::from_mut_slice(&mut cv));
                for i in 0..count {
                    p[i] ^= cv[i];
                }
                buffer[index..index + count].copy_from_slice(&p[..count]);
                count = 0;
            }
        }
    }

    /// Decrypts given buffer of bytes in place according to Satel ETHM-1 CBC/stream cipher specification.
    pub fn decrypt(&self, buffer: &mut [u8]) {
        let mut cv = [0u8; 16];
        self.cipher.encrypt_block(GenericArray::from_mut_slice(&mut cv));

        let mut index = 0;
        let mut count = buffer.len();

        while count > 0 {
            if count > 15 {
                count -= 16;
                let mut temp = [0u8; 16];
                temp.copy_from_slice(&buffer[index..index + 16]);
                let mut c = [0u8; 16];
                c.copy_from_slice(&buffer[index..index + 16]);
                self.cipher.decrypt_block(GenericArray::from_mut_slice(&mut c));
                for i in 0..16 {
                    c[i] ^= cv[i];
                    cv[i] = temp[i];
                }
                buffer[index..index + 16].copy_from_slice(&c);
                index += 16;
            } else {
                let mut c = [0u8; 16];
                c[..count].copy_from_slice(&buffer[index..index + count]);
                self.cipher.encrypt_block(GenericArray::from_mut_slice(&mut cv));
                for i in 0..count {
                    c[i] ^= cv[i];
                }
                buffer[index..index + count].copy_from_slice(&c[..count]);
                count = 0;
            }
        }
    }
}

/// Helper function to encrypt a single message buffer into an encrypted wire frame.
#[allow(dead_code)]
pub fn encrypt_data(key: &[u8; AES_KEY_SIZE], plaintext: &[u8]) -> Vec<u8> {
    if plaintext.is_empty() {
        return Vec::new();
    }
    let cipher = SatelAesCipher::new(*key);
    let bytes_count = std::cmp::max(16, 6 + plaintext.len());
    let mut data = vec![0u8; bytes_count];
    data[6..6 + plaintext.len()].copy_from_slice(plaintext);
    cipher.encrypt(&mut data);

    let mut out = Vec::with_capacity(1 + bytes_count);
    out.push(bytes_count as u8);
    out.extend_from_slice(&data);
    out
}

/// Helper function to decrypt a single message buffer from an encrypted wire frame.
#[allow(dead_code)]
pub fn decrypt_data(key: &[u8; AES_KEY_SIZE], ciphertext: &[u8]) -> Result<Vec<u8>, SatelError> {
    if ciphertext.is_empty() {
        return Ok(Vec::new());
    }
    if ciphertext.len() < 17 {
        return Err(SatelError::InvalidEncryptedDataLength(ciphertext.len()));
    }
    let cipher = SatelAesCipher::new(*key);
    let len_prefix = ciphertext[0] as usize;
    if ciphertext.len() < 1 + len_prefix || len_prefix < 16 {
        return Err(SatelError::InvalidEncryptedDataLength(ciphertext.len()));
    }
    let mut payload = ciphertext[1..1 + len_prefix].to_vec();
    cipher.decrypt(&mut payload);
    Ok(payload[6..].to_vec())
}

/// Transparent asynchronous stream wrapper implementing Satel ETHM-1 encrypted session protocol.
///
/// Wire packet structure:
/// `[1 byte length prefix] [AES-192 encrypted payload]`
///
/// Encrypted payload layout before encryption:
/// - bytes 0..2: 16-bit random value
/// - bytes 2..4: 16-bit rolling counter (big-endian)
/// - byte 4: `id_s` (random session sender ID for this message)
/// - byte 5: `id_r` (last received `id_s` from panel)
/// - bytes 6..: Plaintext Satel frame (e.g. `0xFE 0xFE ... 0xFE 0x0D`)
pub struct EncryptedStream<S> {
    inner: S,
    cipher: SatelAesCipher,
    id_s: u8,
    id_r: u8,
    rolling_counter: u16,
    read_buffer: BytesMut,
    raw_read_buffer: BytesMut,
    expected_resp_len: Option<usize>,
    write_buffer: BytesMut,
}

impl<S> EncryptedStream<S> {
    /// Creates a new `EncryptedStream` wrapping the inner stream with the given AES-192 key.
    pub fn new(inner: S, key: [u8; AES_KEY_SIZE]) -> Self {
        Self {
            inner,
            cipher: SatelAesCipher::new(key),
            id_s: 0,
            id_r: 0,
            rolling_counter: 0,
            read_buffer: BytesMut::with_capacity(512),
            raw_read_buffer: BytesMut::with_capacity(512),
            expected_resp_len: None,
            write_buffer: BytesMut::with_capacity(512),
        }
    }

    /// Consumes the wrapper and returns the inner stream.
    #[allow(dead_code)]
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Returns a reference to the inner stream.
    #[allow(dead_code)]
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Returns a mutable reference to the inner stream.
    #[allow(dead_code)]
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    fn pseudo_random_u16(&self) -> u16 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        (nanos & 0xFFFF) as u16
    }

    fn pseudo_random_u8(&self) -> u8 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        ((nanos >> 16) & 0xFF) as u8
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for EncryptedStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        loop {
            // 1. If we have decrypted plaintext data available, yield it to the caller
            if !this.read_buffer.is_empty() {
                let to_copy = std::cmp::min(this.read_buffer.len(), buf.remaining());
                buf.put_slice(&this.read_buffer[..to_copy]);
                this.read_buffer.advance(to_copy);
                return Poll::Ready(Ok(()));
            }

            // 2. Read raw bytes from the underlying stream
            let mut tmp_buf = [0u8; 512];
            let mut read_buf = ReadBuf::new(&mut tmp_buf);

            match Pin::new(&mut this.inner).poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(())) => {
                    let n = read_buf.filled().len();
                    if n == 0 {
                        // EOF reached
                        return Poll::Ready(Ok(()));
                    }
                    this.raw_read_buffer.extend_from_slice(read_buf.filled());

                    // 3. Process incoming packets
                    loop {
                        if this.expected_resp_len.is_none() {
                            if this.raw_read_buffer.is_empty() {
                                break;
                            }
                            let len_prefix = this.raw_read_buffer[0] as usize;
                            this.raw_read_buffer.advance(1);
                            this.expected_resp_len = Some(len_prefix);
                        }

                        if let Some(target_len) = this.expected_resp_len {
                            if this.raw_read_buffer.len() < target_len {
                                // Need more bytes for this encrypted packet
                                break;
                            }

                            let mut packet_data = this.raw_read_buffer.split_to(target_len).to_vec();
                            this.expected_resp_len = None;

                            // Decrypt payload
                            this.cipher.decrypt(&mut packet_data);

                            if packet_data.len() >= 6 {
                                this.id_r = packet_data[4];
                                // Plaintext is from offset 6 onward
                                this.read_buffer.extend_from_slice(&packet_data[6..]);
                            }
                        }
                    }

                    // If plaintext data became available, yield it
                    if !this.read_buffer.is_empty() {
                        let to_copy = std::cmp::min(this.read_buffer.len(), buf.remaining());
                        buf.put_slice(&this.read_buffer[..to_copy]);
                        this.read_buffer.advance(to_copy);
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for EncryptedStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        // 1. Flush any previously buffered output
        while !this.write_buffer.is_empty() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.write_buffer) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write encrypted data to stream",
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    this.write_buffer.advance(n);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        // 2. Wrap incoming plaintext message into Satel encrypted frame
        let bytes_count = std::cmp::max(16, 6 + buf.len());
        let mut data = vec![0u8; bytes_count];

        let random_val = this.pseudo_random_u16();
        data[0] = (random_val >> 8) as u8;
        data[1] = (random_val & 0xFF) as u8;
        data[2] = (this.rolling_counter >> 8) as u8;
        data[3] = (this.rolling_counter & 0xFF) as u8;
        this.id_s = this.pseudo_random_u8();
        data[4] = this.id_s;
        data[5] = this.id_r;
        this.rolling_counter = this.rolling_counter.wrapping_add(1);

        data[6..6 + buf.len()].copy_from_slice(buf);

        // Encrypt in-place
        this.cipher.encrypt(&mut data);

        // Prepare wire output: [length prefix] + [encrypted payload]
        this.write_buffer.clear();
        this.write_buffer.reserve(1 + bytes_count);
        this.write_buffer.extend_from_slice(&[bytes_count as u8]);
        this.write_buffer.extend_from_slice(&data);

        // 3. Write to the inner stream
        while !this.write_buffer.is_empty() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.write_buffer) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write encrypted data to stream",
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    this.write_buffer.advance(n);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => break,
            }
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        while !this.write_buffer.is_empty() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.write_buffer) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to flush encrypted buffer to stream",
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    this.write_buffer.advance(n);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        while !this.write_buffer.is_empty() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.write_buffer) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to flush encrypted buffer before shutdown",
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    this.write_buffer.advance(n);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_derive_key_exact_12_chars() {
        let key = derive_aes_key("123456789012");
        let expected_half = b"123456789012";
        assert_eq!(&key[0..12], expected_half);
        assert_eq!(&key[12..24], expected_half);
    }

    #[test]
    fn test_derive_key_short() {
        let key = derive_aes_key("ABC");
        let mut expected_half = [0x20u8; 12];
        expected_half[0..3].copy_from_slice(b"ABC");
        assert_eq!(&key[0..12], &expected_half);
        assert_eq!(&key[12..24], &expected_half);
    }

    #[test]
    fn test_derive_key_empty() {
        let key = derive_aes_key("");
        assert_eq!(key, [0x20u8; 24]);
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip_data() {
        let key = derive_aes_key("TestKey123");
        let original = vec![0xFE, 0xFE, 0x7E, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0xFE, 0x0D];

        let encrypted = encrypt_data(&key, &original);
        assert!(encrypted.len() >= 17);
        assert_ne!(&encrypted[1..], &original);

        let decrypted = decrypt_data(&key, &encrypted).unwrap();
        assert_eq!(&decrypted[..original.len()], &original[..]);
    }

    #[tokio::test]
    async fn test_encrypted_stream_roundtrip() {
        let (client_raw, server_raw) = tokio::io::duplex(1024);
        let key = derive_aes_key("SecretKey123");

        let mut client = EncryptedStream::new(client_raw, key);
        let mut server = EncryptedStream::new(server_raw, key);

        let message = b"Hello Satel Integra encrypted world!";
        client.write_all(message).await.unwrap();
        client.flush().await.unwrap();

        let mut received = vec![0u8; message.len()];
        server.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, message);
    }

    #[tokio::test]
    async fn test_encrypted_stream_chunked_packets() {
        let (client_raw, mut server_raw) = tokio::io::duplex(1024);
        let key = derive_aes_key("SecretKey123");

        let mut client = EncryptedStream::new(client_raw, key);

        let message = b"Chunked test 123";
        client.write_all(message).await.unwrap();
        client.flush().await.unwrap();

        let (mut feed_writer, feed_reader) = tokio::io::duplex(1024);
        let mut decrypting_stream = EncryptedStream::new(feed_reader, key);

        tokio::spawn(async move {
            let mut byte = [0u8; 1];
            while let Ok(_) = server_raw.read_exact(&mut byte).await {
                if feed_writer.write_all(&byte).await.is_err() {
                    break;
                }
            }
        });

        let mut received = vec![0u8; message.len()];
        decrypting_stream.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, message);
    }
}
