use dora_message::metadata::ArrowTypeInfo;
use dora_node_api::{self, DoraNode, MetadataParameters};
use eyre::{Context, bail};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::File,
    time::{Duration, Instant},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct RecordHeader {
    pub type_info: ArrowTypeInfo,
    pub parameters: MetadataParameters,
}

fn main() -> eyre::Result<()> {
    let (mut node, _events) = DoraNode::init_from_env()?;

    let fd = File::open(env::var("MCAP_FILE").unwrap_or_else(|_| "record.mcap".into()))
        .context("couldn't open MCAP file")?;
    let mapped = unsafe { Mmap::map(&fd) }.context("could't map MCAP file")?;
    if let Some(delay_ms) = env::var("DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        std::thread::sleep(Duration::from_millis(delay_ms));
    }

    let stream = mcap::MessageStream::new(&mapped)?;
    let mut schema_verified = false;

    let mut start = None;
    let mut channel_enabled = vec![];
    for message in stream {
        let message = message?;
        if !schema_verified {
            let schema_version = message.channel.schema.as_ref().map(|s| s.name.as_str());
            if !schema_version.is_some_and(|s| s.ends_with("v1")) {
                bail!("Unsupported schema version: {:?}", schema_version);
            }
            schema_verified = true;
        }
        let channel_idx = message.channel.id as usize - 1;
        if channel_enabled.len() <= channel_idx {
            assert_eq!(channel_enabled.len(), channel_idx);
            let enabled = node.node_config().outputs.contains(&message.channel.topic);
            channel_enabled.push(enabled);
        }
        if !channel_enabled[channel_idx] {
            continue;
        }

        let publish_time = match start.as_ref() {
            Some((instant, start)) => {
                *instant + Duration::from_millis(message.publish_time - start)
            }
            None => {
                let now = Instant::now();
                start = Some((now, message.publish_time));
                now
            }
        };
        if let Some(delay) = publish_time.checked_duration_since(Instant::now()) {
            std::thread::sleep(delay);
        }

        let (header, header_len) = bincode::serde::decode_from_slice::<RecordHeader, _>(
            &message.data,
            bincode::config::standard(),
        )
        .context("couldn't decode record header")?;
        let data = &message.data[header_len..];
        node.send_typed_output(
            message.channel.topic.to_string().into(),
            header.type_info,
            header.parameters,
            data.len(),
            |dst| dst.copy_from_slice(data),
        )?;
    }

    Ok(())
}
