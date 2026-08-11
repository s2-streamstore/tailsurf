//! Minimal SDK lifecycle example using a private stream and separate owner, write, and read tokens.

use std::env;

use tailsurf::{
    TokenPermissions, TsfClient, WriterId,
    protocol::{
        rest::{CreateStreamRequest, Visibility},
        ws::{
            ReadStart, ReadStreamOptions, WriteStreamOptions,
            frame::{PartHeader, RecordFormat},
        },
    },
};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = if let Ok(api_url) = env::var("TSF_API_URL") {
        TsfClient::with_api_base_url(Url::parse(&api_url)?)
    } else {
        TsfClient::new()
    };

    let created = client
        .create_stream(&CreateStreamRequest {
            visibility: Visibility::Private,
            retention_secs: None,
            issue_tokens: Some(vec![
                TokenPermissions::owner(),
                TokenPermissions::write(),
                TokenPermissions::read(),
            ]),
        })
        .await?;
    let owner_token = created
        .tokens
        .iter()
        .find(|token| token.permissions.allows_owner())
        .expect("owner token")
        .token
        .clone();
    let write_token = created
        .tokens
        .iter()
        .find(|token| token.permissions.allows_write() && !token.permissions.allows_owner())
        .expect("write token")
        .token
        .clone();
    let read_token = created
        .tokens
        .iter()
        .find(|token| token.permissions.allows_read() && !token.permissions.allows_owner())
        .expect("read token")
        .token
        .clone();

    let writer = client
        .connect_producer(WriteStreamOptions::with_stream_token(
            created.stream_id,
            WriterId::new_random(),
            &write_token,
        ))
        .await?;
    let ticket = writer
        .submit(tailsurf::WriteRecord::new(
            0,
            PartHeader::unsplit(),
            RecordFormat::Transcript,
            b"hello from tailsurf\n",
        ))
        .await?;
    let _receipt = ticket.await?;
    writer.close().await?;

    let mut read_request = ReadStreamOptions::new(created.stream_id).with_stream_token(&read_token);
    read_request.start = Some(ReadStart::SeqNum(0));
    read_request.count = Some(1);
    let mut reader = client.connect_reader(read_request).await?;
    if let Some(record) = reader.next_record().await? {
        print!("{}", String::from_utf8_lossy(&record.data));
    }

    client
        .delete_stream(&created.stream_id, &owner_token)
        .await?;

    Ok(())
}
