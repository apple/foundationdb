use super::frame::{Frame, FrameDecoder, FrameEncoder};
use super::Result;
use tokio_util::codec::{Decoder, Encoder};

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
    writer: BufWriter<W>,
    buf: BytesMut,
    frame_encoder: FrameEncoder,
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
        buffer: BytesMut::new(),
        frame_decoder: FrameDecoder::new(),
    };
    let writer = ConnectionWriter {
        writer: BufWriter::new(writer),
        buf: BytesMut::new(),
        frame_encoder: FrameEncoder::new(),
    };
    (reader, writer)
}

impl<W: AsyncWrite + Unpin> ConnectionWriter<W> {
    pub async fn write_frame(&mut self, frame: Frame) -> Result<()> {
        self.frame_encoder.encode(frame, &mut self.buf)?; // TODO: Reuse buf!
        self.writer.write_all(&self.buf).await?;
        self.buf.clear();
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
