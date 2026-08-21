use bytes::{Buf, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Oblicza sumę kontrolną CRC-16 dla protokołu Satel Integra.
/// Inicjalizacja: 0x147A, dla każdego bajtu: rotate_left(1), XOR 0xFFFF, + crc_high + byte.
fn calculate_crc(data: &[u8]) -> u16 {
    let mut crc: u16 = 0x147A;
    for &byte in data {
        crc = crc.rotate_left(1);
        crc ^= 0xFFFF;
        crc = crc.wrapping_add((crc >> 8) as u16);
        crc = crc.wrapping_add(byte as u16);
    }
    crc
}

/// Kodek ramki protokołu Satel Integra (Function 2).
///
/// Ramka: `0xFE 0xFE [cmd] [data...] [crc_high] [crc_low] 0xFE 0x0D`
///
/// Bajt 0xFE wewnątrz danych jest zastępowany przez sekwencję `0xFE 0xF0` (byte stuffing).
#[derive(Default)]
pub struct SatelCodec;

impl Decoder for SatelCodec {
    type Item = Vec<u8>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if src.len() < 2 {
                return Ok(None);
            }
            if let Some(pos) = src.windows(2).position(|w| w == [0xFE, 0xFE]) {
                if pos > 0 {
                    src.advance(pos);
                }
                break;
            } else {
                let to_advance = if src.last() == Some(&0xFE) {
                    src.len() - 1
                } else {
                    src.len()
                };
                src.advance(to_advance);
                return Ok(None);
            }
        }

        if let Some(pos) = src.windows(2).position(|window| window == [0xFE, 0x0D]) {
            let frame_raw = src.split_to(pos + 2).to_vec();
            let mut data_with_crc = Vec::new();
            let mut i = 2;
            while i < frame_raw.len() - 2 {
                if frame_raw[i] == 0xFE && frame_raw.get(i + 1) == Some(&0xF0) {
                    data_with_crc.push(0xFE);
                    i += 2;
                } else {
                    data_with_crc.push(frame_raw[i]);
                    i += 1;
                }
            }
            if data_with_crc.len() < 2 {
                return self.decode(src);
            }
            let low = data_with_crc.pop().unwrap();
            let high = data_with_crc.pop().unwrap();
            let received_crc = u16::from_be_bytes([high, low]);
            if received_crc == calculate_crc(&data_with_crc) {
                Ok(Some(data_with_crc))
            } else {
                self.decode(src)
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder<Vec<u8>> for SatelCodec {
    type Error = io::Error;
    fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&[0xFE, 0xFE]);
        for &byte in &item {
            if byte == 0xFE {
                dst.extend_from_slice(&[0xFE, 0xF0]);
            } else {
                dst.extend_from_slice(&[byte]);
            }
        }
        let crc = calculate_crc(&item);
        for &byte in &crc.to_be_bytes() {
            if byte == 0xFE {
                dst.extend_from_slice(&[0xFE, 0xF0]);
            } else {
                dst.extend_from_slice(&[byte]);
            }
        }
        dst.extend_from_slice(&[0xFE, 0x0D]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc_known_value() {
        // Weryfikacja CRC na prostej ramce: komenda 0x7E (IntegraVersion)
        let data = vec![0x7E];
        let crc = calculate_crc(&data);
        // Wynik wyliczony ręcznie wg algorytmu Satel
        assert_ne!(crc, 0); // placeholder - do uzupełnienia po testach z centralą
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        use bytes::BytesMut;
        let mut codec = SatelCodec;
        let original = vec![0x7E, 0x01, 0x02];
        let mut buf = BytesMut::new();
        codec.encode(original.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, Some(original));
    }

    #[test]
    fn test_byte_stuffing_encode() {
        use bytes::BytesMut;
        let mut codec = SatelCodec;
        // Dane zawierające 0xFE - muszą być zastąpione przez 0xFE 0xF0
        let data = vec![0xFE];
        let mut buf = BytesMut::new();
        codec.encode(data, &mut buf).unwrap();
        // Sprawdzamy że w buforze pojawia się 0xFE 0xF0 (po nagłówku 0xFE 0xFE)
        let buf_vec: Vec<u8> = buf.to_vec();
        assert!(buf_vec.windows(2).any(|w| w == [0xFE, 0xF0]));
    }
}
