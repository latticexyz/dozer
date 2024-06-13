use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum ApiError {
    User(String),
    Server(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiErrorMessage {
    pub msg: String,
}

impl From<serde_json::Error> for ApiError {
    fn from(err: serde_json::Error) -> Self {
        ApiError::Server(err.into())
    }
}

impl From<tokio_postgres::Error> for ApiError {
    fn from(err: tokio_postgres::Error) -> Self {
        ApiError::Server(err.into())
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            Self::User(msg) => (StatusCode::BAD_REQUEST, msg),
            Self::Server(e) => {
                tracing::error!(%e, "server-error={:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    String::from("server error"),
                )
            }
        };
        let m = ApiErrorMessage {
            msg: String::from(message),
        };
        (status, axum::Json(m)).into_response()
    }
}

impl From<eyre::Report> for ApiError {
    fn from(value: eyre::Report) -> Self {
        ApiError::Server(value.into())
    }
}
