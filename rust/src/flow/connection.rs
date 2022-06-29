use super::frame::{ConnectPacket, Frame, FrameDecoder, FrameEncoder};
use super::Result;
use tokio_util::codec::{Decoder, Encoder};
use std::net::SocketAddr;

use bytes::BytesMut;
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter, ReadHalf, WriteHalf,
};

pub struct ConnectionReader<R: AsyncRead> {
    reader: R,
    buffer: BytesMut,
    frame_decoder: FrameDecoder,
}

pub struct ConnectionWriter<W: AsyncWrite> {
    listen_addr: Option<SocketAddr>,
    writer: BufWriter<W>,
    buf: BytesMut,
    frame_encoder: FrameEncoder,
}

pub async fn new<C: AsyncRead + AsyncWrite + Unpin + Send>(
    listen_addr: Option<SocketAddr>, c: C,
) -> Result<(
    ConnectionReader<ReadHalf<C>>,
    ConnectionWriter<WriteHalf<C>>,
    ConnectPacket,
)> {
    let (reader, writer) = tokio::io::split(c);
    let mut reader = ConnectionReader {
        reader,
        buffer: BytesMut::new(),
        frame_decoder: FrameDecoder::new(),
    };
    let mut writer = ConnectionWriter {
        listen_addr,
        writer: BufWriter::new(writer),
        buf: BytesMut::new(),
        frame_encoder: FrameEncoder::new(),
    };

    writer.write_connnect_packet().await?;
    let conn_packet = reader.read_connect_packet().await?;
    Ok((reader, writer, conn_packet))
}

impl<W: AsyncWrite + Unpin> ConnectionWriter<W> {
    pub async fn write_connnect_packet(&mut self) -> Result<()> {
        ConnectPacket::new(self.listen_addr)?.serialize(&mut self.buf)?;
        self.flush().await?;
        Ok(())
    }

    pub async fn write_frame(&mut self, frame: Frame) -> Result<()> {
        self.frame_encoder.encode(frame, &mut self.buf)?;
        if self.buf.len() > 1_000_000 {
            self.writer.write_all(&self.buf).await?;
            self.buf.clear();
        }
        Ok(())
    }
    pub async fn flush(&mut self) -> Result<()> {
        self.writer.write_all(&self.buf).await?;
        self.buf.clear();
        self.writer.flush().await?;
        Ok(())
    }
}

impl<R: AsyncRead + Unpin> ConnectionReader<R> {
    pub async fn read_connect_packet(&mut self) -> Result<ConnectPacket> {
        loop {
            if let Some(connect_packet) = ConnectPacket::deserialize(&mut self.buffer)? {
                return Ok(connect_packet);
            }
            if 0 == self.reader.read_buf(&mut self.buffer).await? {
                // eof
                return Err(format!("connection closed during handshake!").into());
            }
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.frame_decoder.decode(&mut self.buffer)? {
                return Ok(Some(frame));
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
