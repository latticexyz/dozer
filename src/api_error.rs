use axum::{
    extract::{rejection::JsonRejection, FromRequest, MatchedPath},
    http::StatusCode,
    RequestPartsExt,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub struct Json<T>(pub T);

#[axum::async_trait]
impl<S, T> FromRequest<S> for Json<T>
where
    axum::Json<T>: FromRequest<S, Rejection = JsonRejection>,
    S: Send + Sync,
{
    type Rejection = (StatusCode, axum::Json<Value>);

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        let (mut parts, body) = req.into_parts();
        let path = parts
            .extract::<MatchedPath>()
            .await
            .map(|path| path.as_str().to_owned())
            .ok();

        let req = axum::extract::Request::from_parts(parts, body);

        match axum::Json::<T>::from_request(req, state).await {
            Ok(value) => Ok(Self(value.0)),
            Err(rejection) => {
                let payload = json!({
                    "message": rejection.body_text(),
                    "origin": "custom_extractor",
                    "path": path,
                });
                Err((rejection.status(), axum::Json(payload)))
            }
        }
    }
}

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
