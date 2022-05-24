// Implementation of the flow network protocol.  See flow_transport.md for more information.
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use std::sync::Arc;
use tokio::sync::Semaphore;
use bytes::{Buf, BytesMut};
use std::io::Cursor;

// TODO
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

mod frame;
mod uid;

struct Listener {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    // TODO: Shutdown?
}

const MAX_CONNECTIONS: usize = 250;



pub async fn hello() -> Result<()> {
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", 6789)).await?;
    let mut server = Listener {
        listener,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
    };

    println!("listening");

    loop {
        server.limit_connections.acquire().await.unwrap().forget();
        let socket = server.listener.accept().await?;
        let mut stream = BufWriter::new(socket.0);
        let mut buf = BytesMut::with_capacity(4 * 1024);
        println!("got socket from {}", socket.1);
        loop {
            let count = stream.read_buf(&mut buf).await?;
            // println!("count: {} buf: {:?}", count, &buf[0..count]);
            if count == 0 {
                break
            }
            let mut cur : Cursor<&[u8]> = Cursor::new(&buf);

            frame::get_connect_packet(&mut cur)?;
            loop {
                let frame = frame::get_frame(&mut cur)?;
                if frame.is_none() {
                    break; // XXX loses stream sync after first OS read!
                }
            }
            
        }
    }

    println!("werewwer");
    Ok(())
}


#[test]
fn test_uid() -> Result<()> {
    let s = "0123456789abcdeffedcba9876543210";
    let uid = uid::UID::from_string(s)?;
    let uid_s = uid.to_string();
    assert_eq!(uid_s, s);
    let uid2 = uid::UID::from_string(&uid_s)?;
    assert_eq!(uid, uid2);
    assert_eq!(uid.to_u128(), 0x0123456789abcdeffedcba9876543210);
    Ok(())
}
