use axum::{http::StatusCode, Json};
use eyre::eyre;
use serde::Serialize;

#[derive(Debug)]
pub enum ApiError {
    User(axum::http::StatusCode, String),
    Server(eyre::Report),
}

#[derive(Serialize)]
pub struct ApiErrorMessage {
    msg: String,
}

impl From<tokio_postgres::Error> for ApiError {
    fn from(err: tokio_postgres::Error) -> Self {
        ApiError::Server(eyre!("database-error={}", err.to_string()))
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            Self::User(status, msg) => {
                tracing::error!("user-error={}", msg);
                (status, msg)
            }
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
        (status, Json(m)).into_response()
    }
}

impl From<eyre::Report> for ApiError {
    fn from(value: eyre::Report) -> Self {
        ApiError::Server(value)
    }
}
