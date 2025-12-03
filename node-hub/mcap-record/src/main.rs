use dora_message::{arrow_data::ArrayData, metadata::ArrowTypeInfo};
use dora_node_api::{self, DoraNode, Event, MetadataParameters, arrow::array::Array};
use eyre::ContextCompat;
use mcap::{WriteOptions, records::MessageHeader};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    env,
    fs::File,
    ptr::NonNull,
    time::SystemTime,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct RecordHeader {
    pub type_info: ArrowTypeInfo,
    pub parameters: MetadataParameters,
}

fn find_backing_buffer(
    data: &ArrayData,
    type_info: &ArrowTypeInfo,
) -> Option<(NonNull<u8>, usize)> {
    for buf in data.buffers() {
        dbg!(buf.data_ptr(), buf.len());
    }
    if let (Some(child_data), Some(child_type_info)) =
        (data.child_data().last(), type_info.child_data.last())
    {
        if let Some(result) = find_backing_buffer(child_data, child_type_info) {
            return Some(result);
        }
    }
    if let (Some(buffer), Some(offset)) = (data.buffers().last(), type_info.buffer_offsets.last()) {
        dbg!(offset);
        Some((buffer.data_ptr(), offset.offset + offset.len))
    } else {
        None
    }
}

fn main() -> eyre::Result<()> {
    let (_node, mut events) = DoraNode::init_from_env()?;
    let mut channels = HashMap::new();

    let mut writer = mcap::Writer::with_options(
        File::create(env::var("MCAP_FILE").unwrap_or_else(|_| "record.mcap".into()))?,
        WriteOptions::new(),
    )?;
    let schema_id = writer.add_schema("Dora Record v1", "", &[])?;
    let mut sequence = 0;

    let mut buf = Vec::new();
    while let Some(event) = events.recv() {
        match event {
            Event::Input { id, data, metadata } => {
                use std::collections::hash_map::Entry;
                let channel_id = match channels.entry(id) {
                    Entry::Occupied(id) => *id.get(),
                    Entry::Vacant(entry) => {
                        let channel_id = writer.add_channel(
                            schema_id,
                            entry.key().as_str(),
                            "",
                            &BTreeMap::new(),
                        )?;
                        *entry.insert(channel_id)
                    }
                };

                let timestamp = metadata.timestamp();

                buf.clear();
                let header = RecordHeader {
                    type_info: metadata.type_info,
                    parameters: metadata.parameters,
                };
                bincode::serde::encode_into_std_write(
                    &header,
                    &mut buf,
                    bincode::config::standard(),
                )?;
                // In Dora, each Arrow data is backed by exactly one buffer.
                let (array_start, array_len) =
                    find_backing_buffer(&data.to_data(), &header.type_info)
                        .context("failed to find backing buffer")?;
                unsafe {
                    buf.extend_from_slice(std::slice::from_raw_parts(
                        array_start.as_ptr(),
                        array_len,
                    ));
                }

                let header = MessageHeader {
                    channel_id,
                    sequence,
                    log_time: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map_or(0, |it| it.as_millis() as u64),
                    publish_time: timestamp.get_time().to_duration().as_millis() as u64,
                };
                sequence += 1;

                writer.write_to_known_channel(&header, &buf)?;
                writer.flush()?;
            }
            Event::Error(err) => {
                println!("Error: {err}");
            }
            event => {
                println!("Event: {event:#?}")
            }
        }
    }

    Ok(())
}
