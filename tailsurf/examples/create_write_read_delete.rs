//! Minimal SDK lifecycle example using a private stream and separate owner, write, and read links.

use std::env;

use tailsurf::{
    LinkPermissions, TsfClient, WriterId,
    protocol::{
        rest::{CreateStreamRequest, InitialStreamLink, Visibility},
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
            title: Some("SDK lifecycle".parse()?),
            visibility: Visibility::Private,
            expires_in_secs: None,
            issue_links: Some(vec![
                InitialStreamLink {
                    label: "Owner".parse()?,
                    permissions: LinkPermissions::owner(),
                },
                InitialStreamLink {
                    label: "Example writer".parse()?,
                    permissions: LinkPermissions::write(),
                },
                InitialStreamLink {
                    label: "Example reader".parse()?,
                    permissions: LinkPermissions::read(),
                },
            ]),
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
        .connect_producer(WriteStreamOptions::with_stream_link(
            created.stream_id,
            WriterId::new_random(),
            &write_link_secret,
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

    let mut read_request =
        ReadStreamOptions::new(created.stream_id).with_stream_link(&read_link_secret);
    read_request.start = Some(ReadStart::SeqNum(0));
    read_request.count = Some(1);
    let mut reader = client.connect_reader(read_request).await?;
    if let Some(record) = reader.next_record().await? {
        print!("{}", String::from_utf8_lossy(&record.data));
    }

    client
        .delete_stream(&created.stream_id, &owner_link_secret)
        .await?;

    Ok(())
}
