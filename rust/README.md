# `tailsurf`

`tailsurf` is the supported Rust SDK for [tail.surf](https://tail.surf).

It includes REST operations, resumable SSE reads, and reconnecting WebSocket readers and writers.

## Install

```sh
cargo add tailsurf
cargo add tokio --features macros,rt-multi-thread
```

## Quickstart

```rust,no_run
use tailsurf::{CreateStreamRequest, TsfClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = TsfClient::new();
    let stream = client.create_stream(&CreateStreamRequest::default()).await?;

    println!("{}", stream.stream_id);
    Ok(())
}
```

The default API origin is `https://tail.surf`. Use `TsfClient::with_api_origin` or `TsfClient::with_config` for another deployment.

## Read

`TsfReadSession` reads bounded binary batches. `TsfSseReadSession` provides the same resumable read contract over HTTP event streams.

```rust,no_run
use tailsurf::{LinkSecret, ReadOptions, ReadStart, StreamId, TsfClient};

async fn read_stream(
    client: &TsfClient,
    stream_id: StreamId,
    read_link_secret: LinkSecret,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut options = ReadOptions::new(stream_id).with_link_secret(read_link_secret);
    options.start = Some(ReadStart::SeqNum(0));
    let mut reader = client.connect_reader(options).await?;

    while let Some(batch) = reader.next_batch().await? {
        for record in &batch {
            println!("{}", String::from_utf8_lossy(record.data));
        }
    }

    Ok(())
}
```

The session reconnects from the latest record or caught-up position after transient interruption. A successful WebSocket handshake starts a fresh retry burst.

## Write

`TsfWriter` retains unacknowledged records across bounded reconnects. Records are submitted as a non-empty `AppendBatch`. The writer assigns writer sequence numbers in submission order, so cloned `TsfProducer` handles can submit concurrently without interleaving. `AppendBatch::split_logical` keeps the parts of an oversized logical record contiguous.

An `AppendBatch` is one sequencing and ticket unit, not an atomic service append. The writer may split it across frames. A terminal failure may leave a durable prefix while its ticket returns an error.

Await each `AppendTicket` when you need its durable sequence numbers. `close` stops new submissions and waits for accepted records to settle. A terminal `AppendDurabilityUnknown` means an append may be durable but its acknowledgement could not be recovered.

```rust,no_run
use tailsurf::{AppendBatch, DurableWriterOptions, LinkSecret, RecordFormat, StreamId, TsfClient};

async fn write_stream(
    client: &TsfClient,
    stream_id: StreamId,
    write_link_secret: LinkSecret,
) -> Result<(), Box<dyn std::error::Error>> {
    let writer = client
        .connect_writer(DurableWriterOptions::new(stream_id, write_link_secret))
        .await?;
    let ticket = writer
        .submit(AppendBatch::split_logical(
            RecordFormat::Transcript,
            b"deploy started\n".as_slice(),
        )?)
        .await?;
    let receipts = ticket.await?;
    writer.close().await?;

    println!("durable at sequence {}", receipts[0].seq_num);
    Ok(())
}
```

The [complete example](https://github.com/s2-streamstore/tailsurf/blob/main/rust/examples/create_write_read_delete.rs) creates, writes, reads, and deletes a stream.

## Manage

Management methods require an owner link secret. `list_links` returns one page. `list_all_links` follows pagination and validates the complete inventory.

## Retries and errors

REST mutations use idempotency keys. Use `create_stream_with_idempotency_key` or `create_link_with_idempotency_key` when a logical creation must survive process restarts.

Transient REST and connection failures use the configured bounded `RetryPolicy`. Operations return `TsfClientError`. HTTP failures expose the status, request ID, retry hint, structured API code, and sequence mismatch details when the server provides them.

## Modules

Common client types are re-exported from the crate root. Lower-level codecs, wire models, URL helpers, permissions, and transcript reconstruction remain available in their named modules.

## License

MIT
