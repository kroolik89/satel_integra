use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Stream wrapper tracking physical byte throughput across wire transports.
///
/// Wraps an underlying transport stream and atomically increments `bytes_sent`
/// on write completions and `bytes_received` on read completions.
#[derive(Debug)]
pub struct CountingStream<S> {
    inner: S,
    bytes_sent: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
}

impl<S> CountingStream<S> {
    /// Creates a new `CountingStream` wrapping `inner` with shared atomic byte counters.
    pub fn new(inner: S, bytes_sent: Arc<AtomicU64>, bytes_received: Arc<AtomicU64>) -> Self {
        Self {
            inner,
            bytes_sent,
            bytes_received,
        }
    }

    /// Returns a reference to the underlying stream.
    #[allow(dead_code)]
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Returns a mutable reference to the underlying stream.
    #[allow(dead_code)]
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consumes the `CountingStream`, returning the wrapped stream.
    #[allow(dead_code)]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for CountingStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let len_before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let len_after = buf.filled().len();
                let n = len_after.saturating_sub(len_before);
                self.bytes_received.fetch_add(n as u64, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CountingStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                self.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write_vectored(cx, bufs) {
            Poll::Ready(Ok(n)) => {
                self.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::SatelCodec;
    use bytes::BytesMut;
    use futures::SinkExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::codec::{Decoder, Encoder, Framed};

    #[tokio::test]
    async fn test_counting_stream_duplex_write_10_read_7() {
        let (client, mut server) = tokio::io::duplex(64);
        let bytes_sent = Arc::new(AtomicU64::new(0));
        let bytes_received = Arc::new(AtomicU64::new(0));
        let mut stream = CountingStream::new(client, bytes_sent.clone(), bytes_received.clone());

        // 1. Zapis 10 bajtów
        stream.write_all(b"0123456789").await.unwrap();
        stream.flush().await.unwrap();

        let mut s_buf = [0u8; 10];
        server.read_exact(&mut s_buf).await.unwrap();
        assert_eq!(&s_buf, b"0123456789");
        assert_eq!(bytes_sent.load(Ordering::Relaxed), 10);

        // 2. Odczyt 7 bajtów
        server.write_all(b"abcdefg").await.unwrap();
        server.flush().await.unwrap();

        let mut r_buf = [0u8; 7];
        stream.read_exact(&mut r_buf).await.unwrap();
        assert_eq!(&r_buf, b"abcdefg");
        assert_eq!(bytes_received.load(Ordering::Relaxed), 7);
    }

    #[tokio::test]
    async fn test_framed_counting_stream_wire_bytes_with_0xfe() {
        let (client, mut server) = tokio::io::duplex(256);
        let bytes_sent = Arc::new(AtomicU64::new(0));
        let bytes_received = Arc::new(AtomicU64::new(0));
        let counting_stream = CountingStream::new(client, bytes_sent.clone(), bytes_received.clone());
        let mut framed = Framed::new(counting_stream, SatelCodec::default());

        // Dane zawierające 0xFE (które kodek musi wyescape'ować do 0xFE 0xF0)
        let payload = vec![0x7E, 0xFE, 0x01];

        // Wyznacz wzorcową długość zakodowanej ramki
        let mut ref_codec = SatelCodec::default();
        let mut encoded = BytesMut::new();
        ref_codec.encode(payload.clone(), &mut encoded).unwrap();
        let expected_wire_len = encoded.len() as u64;

        // Wyślij przez Framed<CountingStream>
        framed.send(payload).await.unwrap();

        // Sprawdź czy bytes_sent == długość ramki zakodowanej przez encode (z FE FE, CRC, FE 0D i escape)
        assert_eq!(bytes_sent.load(Ordering::Relaxed), expected_wire_len);

        // Odbierz po stronie serwera i zdekoduj
        let mut server_buf = vec![0u8; expected_wire_len as usize];
        server.read_exact(&mut server_buf).await.unwrap();
        let mut decode_buf = BytesMut::from(&server_buf[..]);
        let decoded = ref_codec.decode(&mut decode_buf).unwrap();
        assert_eq!(decoded, Some(vec![0x7E, 0xFE, 0x01]));
    }
}
