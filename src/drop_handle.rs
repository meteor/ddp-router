use tokio::task::JoinHandle;

pub struct DropHandle<T>(Option<JoinHandle<T>>);

impl<T> DropHandle<T> {
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self(Some(handle))
    }

    #[allow(unused_must_use)] // Ignore the result.
    pub async fn shutdown(mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
            handle.await;
        }
    }
}

impl<T> Drop for DropHandle<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}
