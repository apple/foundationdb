use super::frame::Frame;
use super::Result;

use bytes::BytesMut;
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter, ReadHalf, WriteHalf,
};

// TODO:  Figure out what this is set to on the C++ side.
const MAX_FDB_FRAME_LENGTH: usize = 1024 * 1024;

pub struct ConnectionReader<R: AsyncRead> {
    reader: R,
    buffer: BytesMut,
    reading_connect_packet: bool,
}

pub struct ConnectionWriter<W: AsyncWrite> {
    writer: BufWriter<W>,
}

pub fn new<C: AsyncRead + AsyncWrite + Unpin + Send>(
    c: C,
) -> (
    ConnectionReader<ReadHalf<C>>,
    ConnectionWriter<WriteHalf<C>>,
) {
    let (reader, writer) = tokio::io::split(c);
    let reader = ConnectionReader {
        reader,
        buffer: BytesMut::with_capacity(MAX_FDB_FRAME_LENGTH),
        reading_connect_packet: true,
    };
    let writer = ConnectionWriter {
        writer: BufWriter::new(writer),
    };
    (reader, writer)
}

impl<W: AsyncWrite + Unpin> ConnectionWriter<W> {
    pub async fn send_connect_packet(&mut self) -> Result<()> {
        let connect_packet = super::frame::ConnectPacket::new();
        self.writer.write_all(&connect_packet.as_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }
    pub async fn write_frame_bytes(&mut self, buf: &[u8]) -> Result<()> {
        self.writer.write_all(&buf).await?;
        Ok(())
    }
    pub async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await?;
        Ok(())
    }
}

impl<R: AsyncRead + Unpin> ConnectionReader<R> {
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            if self.reading_connect_packet {
                if let Some(_connect_packet) = super::frame::get_connect_packet(&mut self.buffer)? {
                    self.reading_connect_packet = false;
                    continue;
                }
            } else {
                if let Some(frame) = super::frame::get_frame(&mut self.buffer)? {
                    return Ok(Some(frame));
                }
            }
            if 0 == self.reader.read_buf(&mut self.buffer).await? {
                // eof
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(format!(
                        "connection closed mid-frame {} bytes left: {:x?}",
                        self.buffer[..].len(),
                        &self.buffer[..]
                    )
                    .into());
                }
            }
        }
    }
}
