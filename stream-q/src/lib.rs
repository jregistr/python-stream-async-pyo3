use std::future::Future;
use std::sync::Arc;
use aws_config::BehaviorVersion;
use aws_sdk_qbusiness::error::SdkError;
use aws_sdk_qbusiness::operation::chat::{ChatError, ChatOutput};
use aws_sdk_qbusiness::primitives::event_stream::EventReceiver;
use aws_sdk_qbusiness::types::{ChatInputStream, ChatOutputStream, EndOfInputEvent, TextInputEvent};
use aws_sdk_qbusiness::types::error::{ChatInputStreamError, ChatOutputStreamError};
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use tokio::sync::Mutex;


#[pyfunction]
fn new_q_client<'p>(py: Python<'p>, application_id: String) -> PyResult<&'p PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = aws_sdk_qbusiness::Client::new(&config);
        let client = QBusiness {client, app_id: application_id};
        Ok(client)
    })
}

#[pyclass]
struct QBusiness {
    client: aws_sdk_qbusiness::Client,
    app_id: String
}

#[pymethods]
impl QBusiness {
    fn list_applications<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let list_app_req = self.client.list_applications().send();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let list_app = match list_app_req.await {
                Ok(apps) => apps,
                Err(e) => {
                    let error = format!("{:?}", e);
                    return Err(PyException::new_err(error));
                }
            };

            let apps = list_app.applications.unwrap_or(Vec::new());
            let names = apps.into_iter().filter_map(|app| app.application_id).collect::<Vec<_>>();
            Ok(names)
        })
    }

    fn chat<'p>(&self, py: Python<'p>, query: String, conversation: Option<String>, parent_msg: Option<String>) -> PyResult<&'p PyAny> {
        let chat_req = chat_async(&self.client, &self.app_id, query, conversation, parent_msg);

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let chat_response = chat_req.await
                .map_err(|e| PyException::new_err(format!("{:?}", e.into_service_error())))?;
            let stream = chat_response.output_stream;
            let q_streamer = QChatStream::new(stream);
            Ok(q_streamer)
        })
    }
}

fn chat_async(
    client: &aws_sdk_qbusiness::Client,
    app_id: &str,
    query: String, conversation: Option<String>, parent_msg: Option<String>
) -> impl Future<Output = Result<ChatOutput, SdkError<ChatError>>> {
    let inputs: Vec<Result<_, ChatInputStreamError>> = vec![
        Ok(ChatInputStream::TextEvent(TextInputEvent::builder().user_message(query).build().unwrap())),
        Ok(ChatInputStream::EndOfInputEvent(EndOfInputEvent::builder().build()))
    ];

    let inputs = futures_util::stream::iter(inputs);
    let input = inputs.into();

    client.chat()
        .application_id(app_id)
        .set_conversation_id(conversation)
        .set_parent_message_id(parent_msg)
        .input_stream(input)
        .send()
}

type ChatEventReceiver = EventReceiver<ChatOutputStream, ChatOutputStreamError>;

#[pyclass]
struct QChatStream {
    inner: Arc<Mutex<ChatEventReceiver>>,
}

impl QChatStream {
    fn new(stream_receiver: ChatEventReceiver) -> Self {
        Self { inner: Arc::new(Mutex::new(stream_receiver)) }
    }
}

#[pyclass]
struct Output {
    #[pyo3(get)]
    text: Option<String>,
    chat_id: Option<String>,
    user_msg_id: Option<String>,
    sys_msg_id: Option<String>,
}

#[pymethods]
impl Output {
    fn __repr__(&self) -> String {
        if self.text.is_some() {
            format!("Text({})", &self.text.as_ref().unwrap())
        } else {
            let empty = "".to_string();
            let chat_id = self.chat_id.as_ref().unwrap_or(&empty);
            let sys = self.sys_msg_id.as_ref().unwrap_or(&empty);
            let usr = self.user_msg_id.as_ref().unwrap_or(&empty);
            format!("Metadata{{ChatId: {}, Sys: {}, Usr: {}}}", chat_id, sys, usr)
        }
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl Output {
    fn text(value: String) -> Self {
        Self { text: Some(value), chat_id: None, sys_msg_id: None, user_msg_id: None }
    }

    fn metadata(chat_id: String, user_msg: String, sys_msg: String) -> Self {
        Self { text: None, chat_id: Some(chat_id), user_msg_id: Some(user_msg), sys_msg_id: Some(sys_msg) }
    }
}

#[pymethods]
impl QChatStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Option<PyObject>> {
        let receiver = self.inner.clone();

        let future = pyo3_asyncio::tokio::future_into_py(py, async move {
            let next_event = receiver.lock().await.recv().await;
            let next_event = match next_event {
                Ok(n) => n,
                Err(e) => {
                    let err_msg = e.to_string();
                    return Err(PyException::new_err(err_msg))
                }
            };

            let Some(next_event) = next_event else {
              return Err(PyStopAsyncIteration::new_err("Iterator exhausted"))
            };

            let res = match next_event {
                ChatOutputStream::TextEvent(text) => Output::text(text.system_message.unwrap_or("".to_string())),
                ChatOutputStream::MetadataEvent(metadata) => Output::metadata(
                    metadata.conversation_id.unwrap(),
                    metadata.user_message_id.unwrap(),
                    metadata.system_message_id.unwrap()
                ),
                _ => Output::text("".to_string())
            };

            Ok(Some(res))
        });

        let result = future?;
        Ok(Some(result.into()))
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn stream_q(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(new_q_client, m)?)?;
    m.add_class::<QChatStream>()?;
    Ok(())
}