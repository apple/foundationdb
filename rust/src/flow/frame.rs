use super::uid::UID;
use super::Result;
use bytes::{Buf, BytesMut};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use std::io::Cursor;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug)]
pub struct Frame {
    pub token: UID,
    pub payload: std::vec::Vec<u8>, // TODO - elide copy.  See read_frame: &'a[u8],
}
// // The value does not include the size of `connectPacketLength` itself,
// // but only the other fields of this structure.
// uint32_t connectPacketLength = 0;
// ProtocolVersion protocolVersion; // Expect currentProtocolVersion

// uint16_t canonical_remote_port = 0; // Port number to reconnect to the originating process
// uint64_t connection_id = 0; // Multi-version clients will use the same Id for both connections, other connections
//                            // will set this to zero. Added at protocol Version 0x0FDB00A444020001.

// // IP Address to reconnect to the originating process. Only one of these must be populated.
// uint32_t canonical_remote_ip4 = 0;

// enum connect_packet_flags { FLAG_IPV6 = 1 };
// uint16_t flags = 0;å
// uint8_t canonical_remote_ip6[16] = { 0 };

#[derive(Debug, FromPrimitive, ToPrimitive, PartialEq)]
pub enum ConnectPacketFlags {
    IPV4 = 0,
    IPV6 = 1,
}

#[derive(Debug)]
pub struct ConnectPacket {
    len: u32,
    version_flags: u8, // Really just 4 bits
    version: u64,      // protocol version bytes.  Human readable in hex.
    canonical_remote_port: u16,
    connection_id: u64,
    canonical_remote_ip4: u32,
    connect_packet_flags: ConnectPacketFlags, // 16 bits on wire
    canonical_remote_ip6: [u8; 16],
}

impl ConnectPacket {
    pub fn new() -> Self {
        ConnectPacket {
            len: 28,
            version_flags: 1, // TODO: set these to real values!
            version: 0xfdb00b072000000,
            canonical_remote_port: 6789,
            connection_id: 1,
            canonical_remote_ip4: 0x7f00_0001,
            connect_packet_flags: ConnectPacketFlags::IPV4,
            canonical_remote_ip6: [0; 16],
        }
    }
    pub fn is_ipv6(&self) -> bool {
        self.connect_packet_flags == ConnectPacketFlags::IPV6
    }
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        let len_sz: usize = 4;
        vec.extend_from_slice(&u32::to_le_bytes(0)); // len
        vec.extend_from_slice(&u64::to_le_bytes(
            (self.version_flags as u64) << 60 | self.version,
        ));
        vec.extend_from_slice(&u16::to_le_bytes(self.canonical_remote_port));
        vec.extend_from_slice(&u64::to_le_bytes(self.connection_id));
        vec.extend_from_slice(&u32::to_le_bytes(self.canonical_remote_ip4));
        vec.extend_from_slice(&u16::to_le_bytes(
            self.connect_packet_flags.to_u16().unwrap(),
        ));
        vec.extend_from_slice(&self.canonical_remote_ip6);
        // XXX below is whatever FDB is sending.  Don't know what it is, or what it means!
        // vec.extend_from_slice(&[0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let frame_sz = vec.len();
        vec[0..len_sz].copy_from_slice(&u32::to_le_bytes((frame_sz - len_sz).try_into().unwrap()));
        vec
    }
}

pub fn get_connect_packet(bytes: &mut BytesMut) -> Result<Option<ConnectPacket>> {
    let cur = Cursor::new(&bytes[..]);
    let start: usize = cur.position().try_into()?;
    let src = &cur.get_ref()[start..];

    let len_sz: usize = 4;
    let version_sz = 8; // note that the 4 msb of the version are flags.

    if src.len() < len_sz + version_sz {
        return Ok(None);
    }

    let len = u32::from_le_bytes(src[0..len_sz].try_into()?);
    let frame_length = len_sz + len as usize;
    let src = &src[len_sz..(len_sz + (len as usize))];

    let version = u64::from_le_bytes(src[0..version_sz].try_into()?);
    let src = &src[version_sz..];

    let version_flags: u8 = (version >> (60)).try_into()?;
    let version = version & !(0b1111 << 60);

    let canonical_remote_port_sz = 2;
    let canonical_remote_port = u16::from_le_bytes(src[0..canonical_remote_port_sz].try_into()?);
    let src = &src[canonical_remote_port_sz..];

    let connection_id_sz = 8;
    let connection_id = u64::from_le_bytes(src[0..connection_id_sz].try_into()?);
    let src = &src[connection_id_sz..];

    let canonical_remote_ip4_sz = 4;
    let canonical_remote_ip4 = u32::from_le_bytes(src[0..canonical_remote_ip4_sz].try_into()?);
    let src = &src[canonical_remote_ip4_sz..];

    let connect_packet_flags_sz = 2;
    let connect_packet_flags_u16 = u16::from_le_bytes(src[0..connect_packet_flags_sz].try_into()?);
    let connect_packet_flags = ConnectPacketFlags::from_u16(connect_packet_flags_u16)
        .ok_or::<super::Error>("Bad connect_packet_flags".into())?;
    let mut src = &src[connect_packet_flags_sz..];

    let canonical_remote_ip6_sz = 16;
    let canonical_remote_ip6 = if src.len() >= 16 {
        let slice = &src[0..canonical_remote_ip6_sz];
        src = &src[canonical_remote_ip6_sz..];
        slice
    } else {
        &[0; 16]
    };

    let cp = ConnectPacket {
        len,
        version_flags,
        canonical_remote_port: canonical_remote_port,
        version,
        connection_id: connection_id,
        canonical_remote_ip4: canonical_remote_ip4,
        connect_packet_flags: connect_packet_flags,
        canonical_remote_ip6: canonical_remote_ip6.try_into()?,
    };

    if src.len() > 0 {
        println!("ConnectPacket: {:x?} (trailing garbage(?): {:?}", cp, src);
    }
    bytes.advance(frame_length);
    Ok(Some(cp))
}

pub fn get_frame(bytes: &mut BytesMut) -> Result<Option<Frame>> {
    let cur = Cursor::new(&bytes[..]);
    let start: usize = cur.position().try_into()?;
    let src = &cur.get_ref()[start..];
    let len_sz = 4;
    let checksum_sz = 8;
    let uid_sz = 16;

    if src.len() < (len_sz + checksum_sz + uid_sz) {
        return Ok(None);
    }

    let len = u32::from_le_bytes(src[0..len_sz].try_into()?) as usize;
    let src = &src[len_sz..];
    let frame_length = len_sz + checksum_sz + len;

    let checksum = u64::from_le_bytes(src[0..checksum_sz].try_into()?);
    let src = &src[checksum_sz..];

    if src.len() < len {
        return Ok(None);
    }

    let xxhash = xxh3_64(&src[..len]);

    let uid = UID::new(src[0..uid_sz].try_into()?)?;
    let src = &src[uid_sz..];

    // println!("Got {} {:x} {:?} ({:?}) bytes_left={}", len, checksum, uid, uid.get_well_known_endpoint(), src.len());

    let payload = src[0..(len - uid_sz)].to_vec();
    // println!("Payload: {:?}", &src[0..len]);

    if checksum != xxhash {
        Err("checksum mismatch".into())
    } else {
        bytes.advance(frame_length);

        Ok(Some(Frame {
            token: uid,
            payload,
        }))
    }
}

impl Frame {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut vec = Vec::<u8>::new();
        let len_sz = 4;
        let checksum_sz = 8;
        let uid_sz = 16;
        // let len = uid_sz + self.payload.len();
        //vec.resize(len, 0);
        vec.extend_from_slice(&u32::to_le_bytes(0)); // we compute len below
        vec.extend_from_slice(&u64::to_le_bytes(0)); // we compute checksum below.
        vec.extend_from_slice(&u64::to_le_bytes(self.token.uid[0]));
        vec.extend_from_slice(&u64::to_le_bytes(self.token.uid[1]));
        vec.extend_from_slice(&self.payload[..]);
        // vec.extend_from_slice(&[0xD, 0xE, 0xA, 0xD, 0xB, 0xE, 0xE, 0xF]);
        // vec.extend_from_slice(&[0xD, 0xE, 0xA, 0xD, 0xB, 0xE, 0xE, 0xF]);
        // vec.extend_from_slice(&[0xD, 0xE, 0xA, 0xD, 0xB, 0xE, 0xE, 0xF]);
        let len: u32 = (vec.len() - len_sz - checksum_sz).try_into().unwrap();
        vec[0..len_sz].copy_from_slice(&u32::to_le_bytes(len));
        let xxh3_64 = xxh3_64(&vec[len_sz + checksum_sz..]);
        vec[len_sz..len_sz + checksum_sz].copy_from_slice(&u64::to_le_bytes(xxh3_64));

        // XXX need to figure out how to jam the FileIdentifier in somehow.
        // See flat_buffer.h::save_with_vtables, which does so by writing a 32 bit
        // value, 32 bits after the start of the root(?)
        // Also, note that FDB modifies bytes at position B << 24, or B << 28 of
        // file_identifiers, making them extremely difficult to grep for.
        // The file identifier for Void is this in the source code;
        // p/x 2010442
        // $1 = 0x1ead4a
        //
        // but this on the wire:
        //  p/x    35564874
        // $2 = 0x21ead4a

        // I think (in that example) the other side wants an ErrorOr<Void> because:
        // template <class T>
        // class ErrorOr : public ComposedIdentifier<T, 2> {
        // std::variant<Error, T> value;

        println!(
            "sent len: {}, vec len: {}, checksum: {}, payload: {:x?} send: {:x?}",
            len,
            vec.len(),
            xxh3_64,
            self.payload,
            &vec
        );
        vec
    }
}
