use super::uid::UID;
use super::Result;
use crate::flow::file_identifier::{FileIdentifier, FileIdentifierNames};
use bytes::{Buf, BytesMut};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use std::net::SocketAddr;
use tokio_util::codec::{Decoder, Encoder};
use xxhash_rust::xxh3;

// TODO:  Figure out what this is set to on the C++ side.
const MAX_FDB_FRAME_LENGTH: u32 = 1024 * 1024;

#[derive(Debug)]
pub struct Frame {
    pub token: UID,
    pub checksum: Option<u64>,
    payload_inner: std::vec::Vec<u8>,
    offset: usize, // offset into payload where the "real" data starts.
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
    version_flags: u8, // Really just 4 bits
    pub version: u64,  // protocol version bytes.  Human readable in hex.
    pub canonical_remote_port: u16,
    connection_id: u64,
    pub canonical_remote_ip4: u32,
    connect_packet_flags: ConnectPacketFlags, // 16 bits on wire
    canonical_remote_ip6: [u8; 16],
}

impl ConnectPacket {
    pub fn new(listen_addr: Option<SocketAddr>) -> Result<Self> {
        let (ip4, port) = match listen_addr {
            None => (0, 0),
            Some(SocketAddr::V4(v4)) => (u32::from_be_bytes(v4.ip().octets()), v4.port()),
            Some(_) => {
                return Err(format!("Unimplemented SocketAddr type: {:?}", listen_addr).into())
            }
        };
        Ok(ConnectPacket {
            version_flags: 1, // TODO: set these to real values!
            version: 0xfdb00b072000000,
            canonical_remote_port: port, // 6789,
            connection_id: 1,
            canonical_remote_ip4: ip4, // 0x7f00_0001,
            connect_packet_flags: ConnectPacketFlags::IPV4,
            canonical_remote_ip6: [0; 16],
        })
    }
    pub fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        //let len_sz: usize = 4;
        let version_sz: usize = 8;
        let port_sz = 2;
        let conn_id_sz = 8;
        let conn_ip4_sz = 4;
        let flags_sz = 2;
        let len: usize = version_sz
            + port_sz
            + conn_id_sz
            + conn_ip4_sz
            + flags_sz
            + self.canonical_remote_ip6.len();
        buf.extend_from_slice(&u32::to_le_bytes(len.try_into()?));
        buf.extend_from_slice(&u64::to_le_bytes(
            (self.version_flags as u64) << 60 | self.version,
        ));
        buf.extend_from_slice(&u16::to_le_bytes(self.canonical_remote_port));
        buf.extend_from_slice(&u64::to_le_bytes(self.connection_id));
        buf.extend_from_slice(&u32::to_le_bytes(self.canonical_remote_ip4));
        buf.extend_from_slice(&u16::to_le_bytes(
            self.connect_packet_flags.to_u16().unwrap(),
        ));
        buf.extend_from_slice(&self.canonical_remote_ip6);
        Ok(())
        //let frame_sz = vec.len();
        //vec[0..len_sz].copy_from_slice(&u32::to_le_bytes((frame_sz - len_sz).try_into().unwrap()));
    }

    pub fn deserialize(bytes: &mut BytesMut) -> Result<Option<ConnectPacket>> {
        let src = &bytes[..];

        let len_sz: usize = 4;
        let version_sz = 8; // note that the 4 msb of the version are flags.

        if src.len() < len_sz + version_sz {
            return Ok(None);
        }

        let len = u32::from_le_bytes(src[0..len_sz].try_into()?);

        if len > MAX_FDB_FRAME_LENGTH {
            return Err("Frame is too long!".into());
        }

        let frame_length = len_sz + len as usize;
        let src = &src[len_sz..(len_sz + (len as usize))];

        let version = u64::from_le_bytes(src[0..version_sz].try_into()?);
        let src = &src[version_sz..];

        let version_flags: u8 = (version >> (60)).try_into()?;
        let version = version & !(0b1111 << 60);

        let canonical_remote_port_sz = 2;
        let canonical_remote_port =
            u16::from_le_bytes(src[0..canonical_remote_port_sz].try_into()?);
        let src = &src[canonical_remote_port_sz..];

        let connection_id_sz = 8;
        let connection_id = u64::from_le_bytes(src[0..connection_id_sz].try_into()?);
        let src = &src[connection_id_sz..];

        let canonical_remote_ip4_sz = 4;
        let canonical_remote_ip4 = u32::from_be_bytes(src[0..canonical_remote_ip4_sz].try_into()?);
        let src = &src[canonical_remote_ip4_sz..];

        let connect_packet_flags_sz = 2;
        let connect_packet_flags_u16 =
            u16::from_le_bytes(src[0..connect_packet_flags_sz].try_into()?);
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
}

fn get_frame(bytes: &mut BytesMut) -> Result<Option<Frame>> {
    let src = &bytes[..];
    let len_sz = 4;
    let checksum_sz = 8;
    let uid_sz = 16;

    if src.len() < (len_sz + checksum_sz + uid_sz) {
        return Ok(None);
    }

    let len = u32::from_le_bytes(src[0..len_sz].try_into()?);

    if len > MAX_FDB_FRAME_LENGTH {
        return Err("Frame is too long!".into());
    }

    let len = len as usize;

    let src = &src[len_sz..];
    let frame_length = len_sz + checksum_sz + len;

    if bytes.len() < frame_length {
        return Ok(None);
    }

    let checksum = Some(u64::from_le_bytes(src[0..checksum_sz].try_into()?));
    let src = &src[checksum_sz..];

    let uid = UID::new(src[0..uid_sz].try_into()?)?;
    let src = &src[uid_sz..];

    // println!("Got {} {:x} {:?} ({:?}) bytes_left={}", len, checksum, uid, uid.get_well_known_endpoint(), src.len());

    let payload = src[0..(len - uid_sz)].to_vec();
    // println!("Payload: {:?}", &src[0..len]);

    bytes.advance(frame_length);

    Ok(Some(Frame {
        token: uid,
        checksum,
        payload_inner: payload,
        offset: 0,
    }))
}

impl Frame {
    pub fn payload(&self) -> &[u8] {
        &self.payload_inner[self.offset..]
    }

    pub fn append_to_buf(&self, buf: &mut BytesMut) -> Result<()> {
        let len_sz = 4;
        let checksum_sz = 8;
        let uid_sz = 16;

        let len_usize = self.payload().len() + uid_sz;
        let len: u32 = len_usize.try_into()?;

        if len > MAX_FDB_FRAME_LENGTH {
            println!("Attempt to serialize frame longer than FDB_MAX_FRAME_LENGTH");
            panic!();
        }

        buf.reserve(buf.len() + len_sz + checksum_sz + len_usize);
        buf.extend_from_slice(&u32::to_le_bytes(len));
        // let checksum_off = buf.len();
        match self.checksum {
            Some(checksum) => buf.extend_from_slice(&u64::to_le_bytes(checksum)),
            None => buf.extend_from_slice(&u64::to_le_bytes(0)),
        };
        buf.extend_from_slice(&u64::to_le_bytes(self.token.uid[0]));
        buf.extend_from_slice(&u64::to_le_bytes(self.token.uid[1]));
        buf.extend_from_slice(&self.payload());

        // if self.checksum.is_none() {
        //     let checksum = xxh3::xxh3_64(&buf[checksum_off+8..]);
        //     buf[checksum_off..checksum_off+8].copy_from_slice(&u64::to_le_bytes(checksum));
        // }

        // println!(
        //     "sent len: {}, vec len: {}, checksum: {}, payload: {:x?} send: {:x?}",
        //     len,
        //     vec.len(),
        //     xxh3_64,
        //     self.payload,
        //     &vec
        // );
        Ok(())
    }
    pub fn peek_file_identifier(&self) -> Result<FileIdentifier> {
        if self.payload().len() < 8 {
            Err(format!(
                "Payload too short to contain file identifier: {:x?}",
                self.payload()
            )
            .into())
        } else {
            let file_identifier = u32::from_le_bytes(self.payload()[4..8].try_into()?);
            FileIdentifier::new_from_wire(file_identifier)
        }
    }

    fn get_u64(&self, table_offset: i32, rel_offset: i32) -> Result<u64> {
        if table_offset % 2 != 0 {
            return Err(format!("Misaligned table at offset: {}", table_offset).into());
        }
        if rel_offset % 2 != 0 {
            return Err(format!("Misaligned offset into table: {}", table_offset).into());
        }
        let off: usize = (table_offset + rel_offset).try_into()?;
        Ok(u64::from_le_bytes(self.payload()[off..off + 8].try_into()?))
    }

    fn get_u32(&self, table_offset: i32, rel_offset: i32) -> Result<u32> {
        if table_offset % 4 != 0 {
            return Err(format!("Misaligned table at offset: {}", table_offset).into());
        }
        if rel_offset % 4 != 0 {
            return Err(format!("Misaligned offset into table: {}", table_offset).into());
        }
        let off: usize = (table_offset + rel_offset).try_into()?;
        Ok(u32::from_le_bytes(self.payload()[off..off + 4].try_into()?))
    }

    fn get_i32(&self, table_offset: i32, rel_offset: i32) -> Result<i32> {
        if table_offset % 4 != 0 {
            return Err(format!("Misaligned table at offset: {}", table_offset).into());
        }
        if rel_offset % 4 != 0 {
            return Err(format!("Misaligned offset into table: {}", table_offset).into());
        }
        let off: usize = (table_offset + rel_offset).try_into()?;
        Ok(i32::from_le_bytes(self.payload()[off..off + 4].try_into()?))
    }

    fn get_i16(&self, table_offset: i32, rel_offset: i32) -> Result<i16> {
        if table_offset % 4 != 0 {
            return Err(format!("Misaligned table at offset: {}", table_offset).into());
        }
        if rel_offset % 2 != 0 {
            return Err(format!("Misaligned offset into table: {}", table_offset).into());
        }
        let off: usize = (table_offset + rel_offset).try_into()?;
        Ok(i16::from_le_bytes(self.payload()[off..off + 2].try_into()?))
    }

    pub fn reverse_engineer_flatbuffer(&self) -> Result<()> {
        println!("Table length = {}", self.payload().len());
        if self.payload().len() < 8 {
            return Err("Too short to be flatbuffer".into());
        }
        let root_table_offset: i32 = self.get_u32(0, 0)?.try_into()?;
        println!("0\t{:x}\t// root table offset", root_table_offset);
        let file_identifier_table = FileIdentifierNames::new()?;
        match file_identifier_table.from_id(&self.peek_file_identifier()?) {
            Ok(id) => {
                println!("4\t{:?}", id);
            }
            Err(_) => {
                println!(
                    "4\t{:?} (unknown file identifier)",
                    self.peek_file_identifier()?
                );
            }
        };

        println!("\tTABLE");
        let vtable_soffset = 0 - self.get_i32(root_table_offset, 0)?;
        println!(
            "{:x}\t{:x} // Offset to vtable start",
            root_table_offset, vtable_soffset
        );
        let vtable_offset: i32 = root_table_offset + vtable_soffset;

        let vtable_len = self.get_i16(vtable_offset, 0)?;
        println!("{:x}\t{:x} // vtable len", vtable_offset, vtable_len);
        let table_len = self.get_i16(vtable_offset, 2)?;
        println!("{:x}\t{:x} // table len", vtable_offset + 2, table_len);
        for i in 2..(vtable_len / 2) {
            let i: i32 = i.into();
            let offset = self.get_i16(vtable_offset, i * 2)?;
            if offset == 0 {
                println!(
                    "{:x}\t0 // field {} is missing",
                    vtable_offset + (i * 2),
                    i - 1
                );
            } else {
                println!(
                    "{:x}\t{:x} // field {} offset",
                    vtable_offset + (i * 2),
                    offset,
                    i - 1
                );
                let offset: i32 = offset.into();
                let val = self.get_u64(root_table_offset, offset)?;
                println!(
                    "{:x}\t{:x} // 64-bits at start of field {}",
                    root_table_offset + offset,
                    val,
                    i - 2
                );
            }
        }

        Ok(())
    }

    fn compute_checksum(&self) -> u64 {
        let mut digest = xxh3::Xxh3::new();
        digest.reset();
        digest.update(&u64::to_le_bytes(self.token.uid[0]));
        digest.update(&u64::to_le_bytes(self.token.uid[1]));
        digest.update(&self.payload());
        digest.digest()
    }

    pub fn validate(&self) -> Result<()> {
        match self.checksum {
            Some(checksum) => {
                if self.compute_checksum() == checksum {
                    Ok(())
                } else {
                    Err("checksum mismatch".into())
                }
            }
            None => Ok(()),
        }
    }

    // Called new_reply because this populates the checksum field instead of
    // letting some downstream thing validate it.
    pub fn new(token: UID, payload: Vec<u8>, offset: usize) -> Self {
        let mut frame = Self {
            token,
            checksum: None,
            payload_inner: payload,
            offset,
        };
        frame.checksum = Some(frame.compute_checksum());
        frame
    }
}

pub struct FrameDecoder {}

impl FrameDecoder {
    pub fn new() -> FrameDecoder {
        FrameDecoder {}
    }
}
impl Decoder for FrameDecoder {
    type Item = Frame;
    type Error = super::Error;

    fn decode(&mut self, src: &mut BytesMut) -> super::Result<Option<Frame>> {
        get_frame(src)
    }
}

pub struct FrameEncoder {}

impl FrameEncoder {
    pub fn new() -> FrameEncoder {
        FrameEncoder {}
    }
}
impl Encoder<&Frame> for FrameEncoder {
    type Error = super::Error;

    fn encode(&mut self, frame: &Frame, dst: &mut BytesMut) -> super::Result<()> {
        frame.append_to_buf(dst)
    }
}
