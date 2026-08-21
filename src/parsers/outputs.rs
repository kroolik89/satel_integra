use crate::error::SatelError;
use crate::state::OutputsStateData;
use chrono::Local;

/// Przetwarza całą ramkę odpowiedzi na komendę 0x17 (Outputs state).
pub fn process_outputs_state(frame: &[u8]) -> Result<OutputsStateData, SatelError> {
    if frame.is_empty() || frame[0] != 0x17 {
        tracing::error!(
            "Nieprawidłowy kod komendy w ramce stanu wyjść: {:02X?}",
            frame.first()
        );
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Dane stanu wyjść zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for &byte in data {
        for bit in 0..8 {
            states.push((byte & (1 << bit)) != 0);
        }
    }

    Ok(OutputsStateData {
        states,
        read_at: Local::now(),
    })
}
