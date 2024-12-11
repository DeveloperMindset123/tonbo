use std::io::Cursor;
use std::ops::Bound;
use std::pin::Pin;
use async_stream::stream;
use bytes::BufMut;
use futures_core::Stream;
use futures_util::StreamExt;
use tonic::{Code, Request, Response, Status};
use crate::DB;
use crate::executor::tokio::TokioExecutor;
use crate::net::proto::{Empty, GetReq, GetResp, InsertReq, RemoveReq, ScanReq, ScanResp};
use crate::net::proto::tonbo_rpc_server::TonboRpc;
use crate::record::{Column, DynRecord};
use crate::serdes::{Decode, Encode};
struct TonboService {
    inner: DB<DynRecord, TokioExecutor>,
    primary_key_index: usize,
}

#[tonic::async_trait]
impl TonboRpc for TonboService {
    async fn get(&self, request: Request<GetReq>) -> Result<Response<GetResp>, Status> {
        let mut req = request.into_inner();
        let key = Column::decode(&mut Cursor::new(&mut req.key)).await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;

        let tuple = self.inner
            .get(&key, |e| Some(e.get().columns))
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        let record = if let Some(tuple) = tuple {
            let mut bytes = Vec::new();
            tuple.encode(&mut Cursor::new(&mut bytes)).await.map_err(|e| Status::new(Code::Internal, e.to_string()))?;
            Some(bytes)
        } else {
            None
        };
        Ok(Response::new(GetResp { record }))
    }

    type ScanStream = Pin<Box<dyn Stream<Item = Result<ScanResp, Status>> + Send + Sync + 'static>>;

    async fn scan(&self, request: Request<ScanReq>) -> Result<Response<Self::ScanStream>, Status> {
        let mut req = request.into_inner();

        let stream = stream! {
            let min = Bound::<Column>::decode(&mut Cursor::new(&mut req.min)).await
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
            let max = Bound::<Column>::decode(&mut Cursor::new(&mut req.max)).await
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;

            let mut stream = self
                .inner
                .scan((min.as_ref(), max.as_ref()), |e| Some(e.get().columns))
                .await;
            while let Some(entry) = stream.next().await {
                let Some(columns) = entry.map_err(|e| Status::new(Code::Internal, e.to_string()))? else { continue };
                let mut bytes = Vec::new();
                columns.encode(&mut Cursor::new(&mut bytes)).await.map_err(|e| Status::new(Code::Internal, e.to_string()))?;
                yield Ok::<ScanResp, Status>(ScanResp { record: bytes });
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn insert(&self, request: Request<InsertReq>) -> Result<Response<Empty>, Status> {
        let mut req = request.into_inner();
        let record = Vec::<Column>::decode(&mut Cursor::new(&mut req.record)).await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;

        self.inner
            .insert(DynRecord::new(record, self.primary_key_index))
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        Ok(Response::new(Empty {}))
    }

    async fn remove(&self, request: Request<RemoveReq>) -> Result<Response<Empty>, Status> {
        let mut req = request.into_inner();
        let column = Column::decode(&mut Cursor::new(&mut req.key)).await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        self.inner
            .remove(column)
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        Ok(Response::new(Empty {}))
    }

    async fn flush(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
       self.inner
           .flush()
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        Ok(Response::new(Empty {}))
    }
}
