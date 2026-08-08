use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;

#[async_trait]
pub trait EndpointHandler {
    async fn listen(self: Arc<Self>, mut shutdown_rx: Receiver<()>) -> Result<()>;
}
