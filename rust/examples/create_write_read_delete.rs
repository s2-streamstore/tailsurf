//! Minimal SDK lifecycle example using a private stream and separate owner, write, and read links.

use std::env;

use tailsurf::{
    CreateStreamRequest, DurableWriterOptions, InitialStreamLink, LinkPermissions, ReadOptions,
    ReadStart, ReadStop, TsfClient, Visibility,
};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = if let Ok(origin) = env::var("TSF_ORIGIN") {
        TsfClient::with_api_origin(Url::parse(&origin)?)?
    } else {
        TsfClient::new()
    };

    let created = client
        .create_stream(&CreateStreamRequest {
            kind: tailsurf::StreamKind::Transcript,
            title: Some("SDK lifecycle".parse()?),
            visibility: Visibility::Private,
            expires_in_seconds: None,
            links: vec![
                InitialStreamLink::new("owner".parse()?, LinkPermissions::owner()),
                InitialStreamLink::new("example-writer".parse()?, LinkPermissions::write()),
                InitialStreamLink::new("example-reader".parse()?, LinkPermissions::read()),
            ],
        })
        .await?;
    let owner_link_secret = created
        .links
        .iter()
        .find(|link| link.permissions.allows_owner())
        .expect("owner link")
        .secret
        .clone();
    let write_link_secret = created
        .links
        .iter()
        .find(|link| link.permissions.allows_write() && !link.permissions.allows_owner())
        .expect("write link")
        .secret
        .clone();
    let read_link_secret = created
        .links
        .iter()
        .find(|link| link.permissions.allows_read() && !link.permissions.allows_owner())
        .expect("read link")
        .secret
        .clone();

    let writer = client
        .connect_writer(
            DurableWriterOptions::new(created.stream_id, write_link_secret)
                .with_expected_next_seq_num(0),
        )
        .await?;
    let ticket = writer.submit(tailsurf::AppendBatch::split_logical(
        b"hello from tailsurf\n".as_slice(),
    )?)?;
    let _receipts = ticket.await?;
    writer.close().await?;

    let mut read_request = ReadOptions::new(created.stream_id).with_link_secret(read_link_secret);
    read_request.start = Some(ReadStart::SeqNum(0));
    read_request.stop = Some(ReadStop {
        count: Some(1),
        ..ReadStop::default()
    });
    let mut reader = client.connect_reader(read_request).await?;
    if let Some(batch) = reader.next_batch().await? {
        for record in &batch {
            print!("{}", String::from_utf8_lossy(record.data));
        }
    }

    client
        .delete_stream(&created.stream_id, &owner_link_secret)
        .await?;

    Ok(())
}
