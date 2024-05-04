// This library enables easy error handling in applications (it's a must-have, IMO)
use anyhow::{bail, Context};

// A unique reference to a slice of memory
use bytes::BytesMut;

// HTTP-related constants
use reqwest::header::{ACCEPT_RANGES, CONTENT_LENGTH, RANGE};
use reqwest::StatusCode;

// Necessary for calculating the downloaded file's SHA256 checksum
use sha2::{Digest, Sha256};

// An instant in time, useful for measuring how long downloads take
use std::time::Instant;

// An atomically reference counted object
use std::sync::Arc;

// A concurrency primitive (see below for how we're using it)
use tokio::sync::Semaphore;

const CONCURRENT_REQUEST_LIMIT: usize = 6;

fn main() -> anyhow::Result<()> {
    // Get the command-line arguments (the first one is the executable's name, so we skip it)
    let mut args = std::env::args().skip(1);

    // First we expect the url
    let Some(url) = args.next() else {
        bail!("url should be provided as the first argument to the program, but no argument was provided")
    };

    // Second is the chunk size
    let Some(chunk_size_str) = args.next() else {
        bail!("chunk size should be provided as the second argument to the program, but no argument was provided")
    };

    let chunk_size_mb: usize = chunk_size_str
        .parse()
        .context("chunk size could not be parsed as an unsigned integer")?;

    // Start a multi-threaded async runtime!
    //
    // Note: we could also use `#[tokio::main]` instead of this, but I wanted to be explicit about
    // the runtime's creation and avoid anything that feels too magic.
    let async_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    // Now block until the async function completes (this is also the moment at which the async
    // function starts executing)
    async_runtime.block_on(run(&url, chunk_size_mb))
}

// Asynchronously download the file and calculate its checksum, printing relevant information in the
// process
async fn run(url: &str, chunk_size_mb: usize) -> anyhow::Result<()> {
    let start = Instant::now();
    let http_client = reqwest::Client::new();

    println!("Downloading `{url}` using up to {CONCURRENT_REQUEST_LIMIT} concurrent requests and {chunk_size_mb} MiB chunks...");

    // Get the file's content length
    let file_size = get_content_length(&http_client, url).await?;
    if file_size == 0 {
        bail!("The file you are trying to download has zero length")
    }

    // Report relevant details about the download
    let file_size_mb = file_size as f64 / 1024.0 / 1024.0;
    let chunk_size_bytes = chunk_size_mb * 1024 * 1024;
    let full_sized_chunks = file_size / chunk_size_bytes as u64;
    let last_smaller_chunk = if file_size % chunk_size_bytes as u64 == 0 {
        0
    } else {
        1
    };
    let total_chunks = full_sized_chunks + last_smaller_chunk;
    println!(
        "The file is {:.2} MiB big and will be downloaded in {total_chunks} chunks...",
        file_size_mb
    );

    // Download the file
    let mut buffer = BytesMut::zeroed(file_size as usize);
    get_in_parallel(&http_client, url, &mut buffer, chunk_size_bytes).await?;
    let download_duration = start.elapsed();

    // Calculate and print the checksum
    let checksum = sha256(&buffer);
    let length_mb = file_size as f64 / 1024. / 1024.;
    println!(
        "File downloaded in {} ms (size = {:.2} MiB; throughput = {:.2} MiB/s)",
        download_duration.as_millis(),
        length_mb,
        length_mb / download_duration.as_secs_f64()
    );
    println!("Checksum: {checksum}");

    Ok(())
}

// Send a HEAD request to obtain the length of the file we want to download (necessary for
// calculating the offsets of the chunks)
async fn get_content_length(http_client: &reqwest::Client, url: &str) -> anyhow::Result<u64> {
    // The function is reasonably straightforward, but there are many things that can go wrong,
    // so it's cluttered by error handling code

    let head_response = http_client
        .head(url)
        .send()
        .await
        .context("HEAD request failed")?;

    head_response
        .error_for_status_ref()
        .context("HEAD request returned non-success status code")?;

    let Some(accept_ranges) = head_response.headers().get(ACCEPT_RANGES) else {
        bail!("Server doesn't support HTTP range requests (missing ACCEPT_RANGES header)");
    };

    let accept_ranges = String::from_utf8_lossy(accept_ranges.as_bytes());
    if accept_ranges != "bytes" {
        bail!("Server doesn't support HTTP range requests (Accept-Ranges = {accept_ranges})");
    }
    let Some(content_length) = head_response.headers().get(CONTENT_LENGTH) else {
        bail!("HEAD response did not contain a Content-Length header");
    };
    let content_length = content_length
        .to_str()
        .context("Content-Length header contained invalid UTF8")?;
    let content_length: u64 = content_length
        .parse()
        .context("Content-Length was not a valid 64-bit unsigned integer")?;

    Ok(content_length)
}

// Send GET requests in parallel to download the file at the specified url
//
// Important: we haven't implemented a mechanism to detect whether the file changed between
// chunk downloads (e.g. do both chunks belong to the same version of the file?)... so don't just
// copy and paste this in a production system :)
async fn get_in_parallel(
    // The HTTP client, used to send the download requests
    http_client: &reqwest::Client,
    // The URL of the file we want to download
    url: &str,
    // A unique reference to a byte slice in memory where the file's bytes will be stored
    buffer: &mut BytesMut,
    // The desired chunk size
    chunk_size: usize,
) -> anyhow::Result<()> {
    // Each chunk download gets its own task, which we will track in this `Vec`. By keeping track of
    // the tasks we can join them later, before returning from this function.
    let mut download_tasks = Vec::new();

    // The semaphore lets us keep the number of parallel requests within the limit below. See the
    // type's documentation for details (https://docs.rs/tokio/1.37.0/tokio/sync/struct.Semaphore.html)
    let semaphore = Arc::new(Semaphore::new(CONCURRENT_REQUEST_LIMIT));

    // After the last chunk is split off, the buffer will be empty, and we will exit the loop (see
    // the loop's body to understand how we are manipulating the `buffer`).
    while !buffer.is_empty() {
        // The last chunk might be smaller than the requested chunk size, so let's cap it
        let this_chunk_size = chunk_size.min(buffer.len());

        // Split off the slice corresponding to this chunk. It will no longer be accessible through
        // the original `buffer`, which will now have its start right after the end of the chunk.
        let mut buffer_slice_for_chunk = buffer.split_to(this_chunk_size);

        // Each download task needs its own copy of the semaphore, the http client and the url
        let chunk_semaphore = semaphore.clone();
        let chunk_http_client = http_client.clone();
        let url = url.to_string();

        // Get the chunk number, for reporting when the download is finished
        let chunk_number = download_tasks.len();

        // Now spawn the chunk download!
        let task = tokio::spawn(async move {
            // Wait until the semaphore lets us pass (i.e. there's room for a download within the
            // concurrency limit)
            let _permit = chunk_semaphore.acquire().await?;

            // Get the chunk using an HTTP range request and store the response body in the buffer
            // slice we just obtained
            let start = Instant::now();
            let range_start = chunk_number * chunk_size;
            get_range(
                &chunk_http_client,
                &url,
                &mut buffer_slice_for_chunk,
                range_start as u64,
            )
            .await?;

            // Report that we are finished
            let duration = start.elapsed();
            let chunk_size_mb = buffer_slice_for_chunk.len() as f64 / 1024.0 / 1024.0;
            println!("* Chunk {chunk_number} downloaded in {} ms (size = {:.2} MiB; throughput = {:.2} MiB/s)", duration.as_millis(), chunk_size_mb, chunk_size_mb / duration.as_secs_f64());

            // Give the buffer slice back, so it can be retrieved when the task is awaited
            Ok::<_, anyhow::Error>(buffer_slice_for_chunk)
        });

        // Keep track of the task, so we can await it and get its buffer slice back
        download_tasks.push(task);
    }

    // Wait for the tasks to complete and stitch the buffer slices back together into a single
    // buffer. Since the slices actually belong together, no copying is necessary!
    for task in download_tasks {
        // The output of the task is the buffer slice that it used
        let buffer_slice = task
            .await
            // Tokio will catch panics for us and other kinds of errors, wrapping the task's return
            // value in a result
            .context("tokio task unexpectedly crashed")?
            // After extracting tokio's result, we get the original return value of the task, which
            // is also a result! Hence the second question mark operator
            .context("chunk download failed")?;

        // This repeatedly appends a split-off slice back into the original buffer
        buffer.unsplit(buffer_slice);
    }

    Ok(())
}

// Sends a GET request to download a chunk of the file at the specified range
async fn get_range(
    http_client: &reqwest::Client,
    url: &str,
    buffer: &mut BytesMut,
    range_start: u64,
) -> anyhow::Result<()> {
    // The function is reasonably straightforward, but there are many things that can go wrong,
    // so it's cluttered by error handling code

    assert!(!buffer.is_empty());

    let range_end_inclusive = range_start as usize + buffer.len() - 1;
    let range_header = format!("bytes={range_start}-{range_end_inclusive}");
    let get_range_response = http_client
        .get(url)
        .header(RANGE, range_header)
        .send()
        .await
        .context("GET request failed")?;
    get_range_response
        .error_for_status_ref()
        .context("GET request returned non-success status code")?;
    if get_range_response.status() != StatusCode::PARTIAL_CONTENT {
        bail!(
            "Response to range request has an unexpected status code (expected {}, found {})",
            StatusCode::PARTIAL_CONTENT,
            get_range_response.status()
        )
    }
    let body = get_range_response
        .bytes()
        .await
        .context("error while streaming body")?;

    assert_eq!(buffer.len(), body.len());
    buffer.copy_from_slice(&body);

    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    format!("{digest:x}")
}
