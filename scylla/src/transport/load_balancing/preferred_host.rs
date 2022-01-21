use super::{LoadBalancingPolicy, Statement};
use super::super::errors::NewSessionError;
use crate::transport::{cluster::ClusterData, node::Node};

use std::sync::Arc;
use std::net::SocketAddr;

use rand::Rng;
use tokio::net::lookup_host;

pub struct PreferredHostPolicy {
    host: SocketAddr,
}

impl PreferredHostPolicy {
    pub fn new<T: Into<SocketAddr>>(host: T) -> Self {
        let host = host.into();
        Self {
            host: host,
        }
    }

    pub async fn new_host<T: AsRef<str>>(host: T) -> Result<Self, NewSessionError> {
        let hostname = host.as_ref();
        let failed_err = NewSessionError::FailedToResolveAddress(hostname.to_string());
        let addrs: Vec<SocketAddr> = match lookup_host(hostname).await {
            Ok(addrs) => addrs.collect(),
            // Use a default port in case of error, but propagate the original error on failure
            Err(e) => lookup_host((hostname, 9042)).await.or(Err(e))?.collect(),
        };

        let mut ret = None;
        for a in addrs {
            match a {
                SocketAddr::V4(_) => return Ok(Self {
                    host: a
                }),
                _ => {
                    ret = Some(a);
                }
            }
        }

        ret.map(|h| Self { host: h }).ok_or(failed_err)
    }
}

impl LoadBalancingPolicy for PreferredHostPolicy {
    fn plan<'a>(
        &self,
        _statement: &Statement,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + Send + Sync + 'a> {
        let mut rng = rand::thread_rng();
        let mut nodes = cluster.all_nodes.clone();
        nodes.sort_by_cached_key(|f|
            if f.address == self.host {
                0usize
            } else {
                rng.gen()
            }
        );
        Box::new(nodes.into_iter())
    }

    fn name(&self) -> String {
        "PreferredHostPolicy".to_string()
    }
}