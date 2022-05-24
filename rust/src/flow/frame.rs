use std::io::Cursor;
use super::{Error,Result};
use super::uid::UID;
use xxhash_rust::xxh3::xxh3_64;

pub struct Frame<'a> {
    token: UID,
    payload: &'a[u8],
}
// // The value does not include the size of `connectPacketLength` itself,
// // but only the other fields of this structure.
// uint32_t connectPacketLength = 0;
// ProtocolVersion protocolVersion; // Expect currentProtocolVersion

// uint16_t canonicalRemotePort = 0; // Port number to reconnect to the originating process
// uint64_t connectionId = 0; // Multi-version clients will use the same Id for both connections, other connections
//                            // will set this to zero. Added at protocol Version 0x0FDB00A444020001.

// // IP Address to reconnect to the originating process. Only one of these must be populated.
// uint32_t canonicalRemoteIp4 = 0;

// enum ConnectPacketFlags { FLAG_IPV6 = 1 };
// uint16_t flags = 0;
// uint8_t canonicalRemoteIp6[16] = { 0 };


#[derive(Debug)]
pub struct ConnectPacket {
    len: u32,
    flags: u8, // Really just 4 bits
    version: u64, // protocol version bytes.  Human readable in hex.
}

pub fn get_connect_packet(cur: &mut Cursor<&[u8]>) -> Result<Option<ConnectPacket>> {
    let start : usize = cur.position().try_into()?;
    let src = &cur.get_ref()[start..];

    let len_sz : usize = 4;
    let version_sz = 8; // note that the 4 msb of the version are flags.

    if src.len() < len_sz + version_sz {
        return Ok(None)
    }

    let len = u32::from_le_bytes(src[0..len_sz].try_into()?);
    let frame_len : u64 = (len_sz + len as usize).try_into()?;
    let src = &src[len_sz..(len_sz + (len as usize))];

    let version = u64::from_le_bytes(src[0..version_sz].try_into()?);
    let src = &src[version_sz..];

    let flags : u8 = (version >> (60)).try_into()?;
    let version = version & !(0b1111 << 60);
    let cp = ConnectPacket {
        len,
        flags,
        version,
    };

    if src.len() > 0 {
        println!("ConnectPacket: {:x?} (trailing garbage(?): {:?}", cp, src);
    }
    cur.set_position(cur.position() + frame_len);
    Ok(Some(cp))
}


pub fn get_frame<'a> (cur: &mut Cursor<&'a [u8]>) -> Result<Option<Frame<'a>>> {
    let start : usize = cur.position().try_into()?;
    let src = &cur.get_ref()[start..];
    let len_sz = 4;
    let checksum_sz = 8;
    let uid_sz = 16;

    if src.len() < (len_sz + checksum_sz + uid_sz) {
        return Ok(None)
    }

    let len = u32::from_le_bytes(src[0..len_sz].try_into()?) as usize;
    let src = &src[len_sz..];
    let frame_length : u64 = (len_sz + checksum_sz + len).try_into()?;

    let checksum = u64::from_le_bytes(src[0..checksum_sz].try_into()?);
    let src = &src[checksum_sz..];

    let xxhash = xxh3_64(&src[..len]);

    let uid = u128::from_le_bytes(src[0..uid_sz].try_into()?);
    let src = &src[uid_sz..];

    // println!("Got {} {:x} ?= {:x} {:x} bytes_left={}", len, checksum, xxhash, uid, src.len());
    if src.len() < len {
        return Ok(None);
    }

    let payload = &src[0..len];
    // println!("Payload: {:?}", &src[0..len]);

    if checksum != xxhash {
        Err("checksum mismatch".into())
    } else {
        cur.set_position(cur.position() + frame_length);

        Ok(Some(Frame{
            token: UID{ uid },
            payload,
        }))
    }
}