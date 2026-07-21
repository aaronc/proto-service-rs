use proto_service::{Code, MetadataMap, Status};

use crate::packet;

pub(crate) fn from_packet_status(status: packet::Status) -> Status {
    Status::new(Code::from(status.code), status.message)
}

pub(crate) fn to_packet_metadata(md: &MetadataMap) -> packet::Metadata {
    packet::Metadata {
        entries: md
            .iter_flat()
            .map(|(key, value)| packet::metadata::Entry {
                key: key.into(),
                value: value.into(),
            })
            .collect(),
    }
}

pub(crate) fn from_packet_metadata(md: packet::Metadata) -> MetadataMap {
    let mut out = MetadataMap::new();
    for entry in md.entries {
        out.append(entry.key, entry.value);
    }
    out
}
