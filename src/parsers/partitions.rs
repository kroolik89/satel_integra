use crate::error::SatelError;
use crate::state::{PartitionsArmedData, PartitionsData};
use chrono::Local;

/// Przetwarza całą ramkę odpowiedzi na komendę 0x09 (Armed partitions suppressed).
pub fn process_partitions_armed_suppressed(frame: &[u8]) -> Result<PartitionsArmedData, SatelError> {
    if frame.is_empty() || frame[0] != 0x09 {
        tracing::error!("Nieprawidłowy kod komendy w ramce uzbrojenia stref (suppressed): {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        tracing::error!("Dane uzbrojenia stref zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsArmedData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x0A (Armed partitions really).
pub fn process_partitions_armed_really(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x0A {
        tracing::error!("Nieprawidłowy kod komendy w ramce uzbrojenia stref (really): {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x0E (Partitions entry time).
pub fn process_partitions_entry_time(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x0E {
        tracing::error!("Nieprawidłowy kod komendy w ramce czasu na wejście: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x0F (Partitions exit time > 10s).
pub fn process_partitions_exit_time_gt_10s(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x0F {
        tracing::error!("Nieprawidłowy kod komendy w ramce czasu na wyjście > 10s: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x10 (Partitions exit time < 10s).
pub fn process_partitions_exit_time_lt_10s(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x10 {
        tracing::error!("Nieprawidłowy kod komendy w ramce czasu na wyjście < 10s: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x13 (Partitions alarm).
pub fn process_partitions_alarm(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x13 {
        tracing::error!("Nieprawidłowy kod komendy w ramce alarmów stref: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x15 (Partitions alarm memory).
pub fn process_partitions_alarm_memory(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x15 {
        tracing::error!("Nieprawidłowy kod komendy w ramce pamięci alarmów stref: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Wspólny helper: parsuje 4 bajty bitowe do Vec<bool> (32 strefy).
fn parse_partition_bits(data: &[u8]) -> Vec<bool> {
    let mut states = Vec::with_capacity(32);
    for &byte in data.iter().take(4) {
        for bit in 0..8 {
            states.push((byte & (1 << bit)) != 0);
        }
    }
    states
}
