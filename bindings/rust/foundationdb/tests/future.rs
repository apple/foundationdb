use foundationdb::*;
use futures::prelude::*;
use std::pin::Pin;
use std::task::{Context, Poll};

mod common;

struct AbortingFuture<F> {
    inner: F,
    polled: bool,
}

impl<T> Future for AbortingFuture<T>
where
    T: Future + Unpin,
{
    type Output = FdbResult<bool>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<FdbResult<bool>> {
        // poll once only
        if !self.polled {
            self.polled = true;
            match Pin::new(&mut self.as_mut().inner).poll(cx) {
                Poll::Pending => (),
                _ => return Poll::Ready(Ok(false)),
            }
        }

        Poll::Ready(Ok(true))
    }
}

#[tokio::test]
async fn test_future_discard() -> FdbResult<()> {
    // dropping a future while it's in the pending state should not crash
    let db = common::database().await?;
    let mut hit_pending = false;
    for _i in 0..=1000 {
        let hit_pending_step = db
            .transact_boxed_local(
                (),
                |trx, ()| {
                    AbortingFuture {
                        inner: trx.get(b"key", false),
                        polled: false,
                    }
                    .boxed_local()
                },
                TransactOption::default(),
            )
            .await?;
        hit_pending = hit_pending || hit_pending_step;
    }

    assert!(hit_pending);

    Ok(())
}
