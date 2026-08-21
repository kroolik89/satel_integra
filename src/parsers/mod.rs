pub mod names;
pub mod outputs;
pub mod partitions;
pub mod system;
pub mod zones;

pub use names::{process_output_name, process_partition_name, process_zone_name};
pub use outputs::process_outputs_state;
pub use partitions::{
    process_partitions_alarm, process_partitions_alarm_memory, process_partitions_armed_really,
    process_partitions_armed_suppressed, process_partitions_entry_time,
    process_partitions_exit_time_gt_10s, process_partitions_exit_time_lt_10s,
};
pub use system::{
    map_trouble_bit, process_auto_read_response, process_ethm_version, process_integra_version,
    process_rtc_and_status, process_troubles,
};
pub use zones::{
    process_zone_temperature, process_zones_alarm, process_zones_alarm_memory,
    process_zones_bypass, process_zones_long_violation_trouble, process_zones_no_violation_trouble,
    process_zones_tamper, process_zones_tamper_alarm, process_zones_tamper_alarm_memory,
    process_zones_violation,
};
