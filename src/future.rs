use crate::body::CompressionBody;
use crate::codec::Codec;
use http::{Response, header};
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pin_project! {
    /// Future for compression service responses.
    pub struct ResponseFuture<F> {
        #[pin]
        inner: F,
        accepted_codec: Option<Codec>,
        min_size: usize,
    }
}

impl<F> ResponseFuture<F> {
    pub(crate) fn new(inner: F, accepted_codec: Option<Codec>, min_size: usize) -> Self {
        Self {
            inner,
            accepted_codec,
            min_size,
        }
    }
}

impl<F, B, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<Response<B>, E>>,
{
    type Output = Result<Response<CompressionBody<B>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(response)) => {
                let response = wrap_response(response, *this.accepted_codec, *this.min_size);
                Poll::Ready(Ok(response))
            }
        }
    }
}

/// Wraps the response body with compression if appropriate.
fn wrap_response<B>(
    response: Response<B>,
    accepted_codec: Option<Codec>,
    min_size: usize,
) -> Response<CompressionBody<B>> {
    let (mut parts, body) = response.into_parts();

    // Never compress responses that have no body: informational (1xx, including
    // 101 Switching Protocols for WebSocket upgrades), 204 No Content, and 304
    // Not Modified. Compressing these would stamp a bogus Content-Encoding header
    // on the response (and, for upgrades, on the handshake).
    if parts.status.is_informational()
        || parts.status == http::StatusCode::NO_CONTENT
        || parts.status == http::StatusCode::NOT_MODIFIED
    {
        return Response::from_parts(parts, CompressionBody::passthrough(body));
    }

    // Determine if we should compress
    let dominated_codec = accepted_codec.filter(|_| {
        !has_content_encoding(&parts.headers)
            && !has_content_range(&parts.headers)
            && !is_uncompressible_content_type(&parts.headers)
            && !is_below_min_size(&parts.headers, min_size)
    });

    let body = if let Some(codec) = dominated_codec {
        // Check for x-accel-buffering: no header or streaming content types
        let always_flush = parts
            .headers
            .get("x-accel-buffering")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("no"))
            || is_streaming_content_type(&parts.headers);

        // Add Content-Encoding header
        parts.headers.insert(
            header::CONTENT_ENCODING,
            header::HeaderValue::from_static(codec.content_encoding()),
        );

        // Remove Content-Length since compressed size is unknown
        parts.headers.remove(header::CONTENT_LENGTH);

        // Remove Accept-Ranges since we can't support ranges on compressed content
        parts.headers.remove(header::ACCEPT_RANGES);

        // Add Accept-Encoding to Vary header if not present
        add_vary_accept_encoding(&mut parts.headers);

        CompressionBody::compressed(body, codec, always_flush)
    } else {
        CompressionBody::passthrough(body)
    };

    Response::from_parts(parts, body)
}

/// Checks if Content-Encoding header is already present.
fn has_content_encoding(headers: &header::HeaderMap) -> bool {
    headers.contains_key(header::CONTENT_ENCODING)
}

/// Checks if Content-Range header is present (range response).
fn has_content_range(headers: &header::HeaderMap) -> bool {
    headers.contains_key(header::CONTENT_RANGE)
}

/// Adds Accept-Encoding to the Vary header if not already present.
fn add_vary_accept_encoding(headers: &mut header::HeaderMap) {
    // Check all Vary headers to see if Accept-Encoding is already present
    for vary in headers.get_all(header::VARY) {
        if let Ok(vary_str) = vary.to_str() {
            let dominated = vary_str.split(',').any(|v| {
                let v = v.trim();
                v.eq_ignore_ascii_case("*") || v.eq_ignore_ascii_case("accept-encoding")
            });
            if dominated {
                return;
            }
        }
    }

    // Append Accept-Encoding to Vary header
    headers.append(
        header::VARY,
        header::HeaderValue::from_static("accept-encoding"),
    );
}

/// Checks if the content type should not be compressed.
fn is_uncompressible_content_type(headers: &header::HeaderMap) -> bool {
    let Some(content_type) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };

    // Skip all images except SVG
    if content_type.starts_with("image/") {
        return !content_type.starts_with("image/svg+xml");
    }

    false
}

/// Checks if the content type requires always flushing (e.g., streaming).
fn is_streaming_content_type(headers: &header::HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/event-stream") || ct.starts_with("application/grpc"))
}

/// Checks if Content-Length is below the minimum size.
fn is_below_min_size(headers: &header::HeaderMap, min_size: usize) -> bool {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .is_some_and(|len| len < min_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::body::CompressState;

    fn make_response(body: &'static str) -> Response<&'static str> {
        Response::new(body)
    }

    fn make_response_with_headers<I>(body: &'static str, headers: I) -> Response<&'static str>
    where
        I: IntoIterator<Item = (&'static str, &'static str)>,
    {
        let mut response = Response::new(body);
        for (name, value) in headers {
            response
                .headers_mut()
                .insert(name, header::HeaderValue::from_static(value));
        }
        response
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_compress_when_accept_encoding_present() {
        let response = make_response("hello world");
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be compressed
        match wrapped.body() {
            crate::body::CompressionBody::Compressed { state, .. } => {
                assert_eq!(state.state(), CompressState::Reading);
            }
            _ => panic!("Expected compressed body"),
        }

        // Should have Content-Encoding header
        assert_eq!(
            wrapped.headers().get(header::CONTENT_ENCODING).unwrap(),
            "gzip"
        );
    }

    #[test]
    fn test_no_compress_when_no_accept_encoding() {
        let response = make_response("hello world");
        let wrapped = wrap_response(response, None, 0);

        // Should be passthrough
        match wrapped.body() {
            crate::body::CompressionBody::Passthrough { .. } => {}
            _ => panic!("Expected passthrough body"),
        }

        // Should NOT have Content-Encoding header
        assert!(wrapped.headers().get(header::CONTENT_ENCODING).is_none());
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_compress_when_content_encoding_present() {
        let response =
            make_response_with_headers("hello world", [("content-encoding", "identity")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be passthrough
        match wrapped.body() {
            crate::body::CompressionBody::Passthrough { .. } => {}
            _ => panic!("Expected passthrough body"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_compress_image_png() {
        let response = make_response_with_headers("PNG data", [("content-type", "image/png")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be passthrough
        match wrapped.body() {
            crate::body::CompressionBody::Passthrough { .. } => {}
            _ => panic!("Expected passthrough body for image/png"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_compress_image_jpeg() {
        let response = make_response_with_headers("JPEG data", [("content-type", "image/jpeg")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be passthrough
        match wrapped.body() {
            crate::body::CompressionBody::Passthrough { .. } => {}
            _ => panic!("Expected passthrough body for image/jpeg"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_compress_image_gif() {
        let response = make_response_with_headers("GIF data", [("content-type", "image/gif")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be passthrough
        match wrapped.body() {
            crate::body::CompressionBody::Passthrough { .. } => {}
            _ => panic!("Expected passthrough body for image/gif"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_compress_image_webp() {
        let response = make_response_with_headers("WebP data", [("content-type", "image/webp")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be passthrough
        match wrapped.body() {
            crate::body::CompressionBody::Passthrough { .. } => {}
            _ => panic!("Expected passthrough body for image/webp"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_compress_image_svg() {
        let response =
            make_response_with_headers("<svg></svg>", [("content-type", "image/svg+xml")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be compressed (SVG is text-based)
        match wrapped.body() {
            crate::body::CompressionBody::Compressed { .. } => {}
            _ => panic!("Expected compressed body for image/svg+xml"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_compress_image_svg_with_charset() {
        let response = make_response_with_headers(
            "<svg></svg>",
            [("content-type", "image/svg+xml; charset=utf-8")],
        );
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be compressed
        match wrapped.body() {
            crate::body::CompressionBody::Compressed { .. } => {}
            _ => panic!("Expected compressed body for image/svg+xml with charset"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_compress_text_html() {
        let response = make_response_with_headers("<html></html>", [("content-type", "text/html")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be compressed
        match wrapped.body() {
            crate::body::CompressionBody::Compressed { .. } => {}
            _ => panic!("Expected compressed body for text/html"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_compress_below_min_size() {
        let response = make_response_with_headers("small", [("content-length", "5")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 100);

        // Should be passthrough (5 < 100)
        match wrapped.body() {
            crate::body::CompressionBody::Passthrough { .. } => {}
            _ => panic!("Expected passthrough body below min size"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_compress_above_min_size() {
        let response =
            make_response_with_headers("large enough content", [("content-length", "200")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 100);

        // Should be compressed (200 >= 100)
        match wrapped.body() {
            crate::body::CompressionBody::Compressed { .. } => {}
            _ => panic!("Expected compressed body above min size"),
        }

        // Content-Length should be removed
        assert!(wrapped.headers().get(header::CONTENT_LENGTH).is_none());
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_compress_unknown_size() {
        // No Content-Length header means unknown size, should compress
        let response = make_response("unknown size content");
        let wrapped = wrap_response(response, Some(Codec::Gzip), 100);

        // Should be compressed (unknown size doesn't trigger min_size check)
        match wrapped.body() {
            crate::body::CompressionBody::Compressed { .. } => {}
            _ => panic!("Expected compressed body for unknown size"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_always_flush_when_x_accel_buffering_no() {
        let response = make_response_with_headers("streaming data", [("x-accel-buffering", "no")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        match wrapped.body() {
            crate::body::CompressionBody::Compressed { state, .. } => {
                assert!(state.always_flush());
            }
            _ => panic!("Expected compressed body"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_always_flush_by_default() {
        let response = make_response("normal data");
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        match wrapped.body() {
            crate::body::CompressionBody::Compressed { state, .. } => {
                assert!(!state.always_flush());
            }
            _ => panic!("Expected compressed body"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_x_accel_buffering_case_insensitive() {
        let response = make_response_with_headers("streaming data", [("x-accel-buffering", "NO")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        match wrapped.body() {
            crate::body::CompressionBody::Compressed { state, .. } => {
                assert!(state.always_flush());
            }
            _ => panic!("Expected compressed body"),
        }
    }

    #[test]
    #[cfg(feature = "brotli")]
    fn test_brotli_content_encoding() {
        let response = make_response("hello world");
        let wrapped = wrap_response(response, Some(Codec::Brotli), 0);

        assert_eq!(
            wrapped.headers().get(header::CONTENT_ENCODING).unwrap(),
            "br"
        );
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_zstd_content_encoding() {
        let response = make_response("hello world");
        let wrapped = wrap_response(response, Some(Codec::Zstd), 0);

        assert_eq!(
            wrapped.headers().get(header::CONTENT_ENCODING).unwrap(),
            "zstd"
        );
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_compress_application_grpc() {
        let response =
            make_response_with_headers("grpc data", [("content-type", "application/grpc")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be compressed with streaming (always_flush)
        match wrapped.body() {
            crate::body::CompressionBody::Compressed { state, .. } => {
                assert!(state.always_flush());
            }
            _ => panic!("Expected compressed body for application/grpc"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_compress_application_grpc_with_suffix() {
        let response =
            make_response_with_headers("grpc data", [("content-type", "application/grpc+proto")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be compressed with streaming (always_flush)
        match wrapped.body() {
            crate::body::CompressionBody::Compressed { state, .. } => {
                assert!(state.always_flush());
            }
            _ => panic!("Expected compressed body for application/grpc+proto"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_compress_application_grpc_web() {
        let response =
            make_response_with_headers("grpc-web data", [("content-type", "application/grpc-web")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        match wrapped.body() {
            crate::body::CompressionBody::Compressed { state, .. } => {
                assert!(state.always_flush());
            }
            _ => panic!("Expected compressed body"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_compress_application_grpc_web_proto() {
        let response = make_response_with_headers(
            "grpc-web data",
            [("content-type", "application/grpc-web+proto")],
        );
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        match wrapped.body() {
            crate::body::CompressionBody::Compressed { state, .. } => {
                assert!(state.always_flush());
            }
            _ => panic!("Expected compressed body"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_always_flush_text_event_stream() {
        let response =
            make_response_with_headers("event: data\n\n", [("content-type", "text/event-stream")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        match wrapped.body() {
            crate::body::CompressionBody::Compressed { state, .. } => {
                assert!(state.always_flush());
            }
            _ => panic!("Expected compressed body"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_always_flush_text_event_stream_with_charset() {
        let response = make_response_with_headers(
            "event: data\n\n",
            [("content-type", "text/event-stream; charset=utf-8")],
        );
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        match wrapped.body() {
            crate::body::CompressionBody::Compressed { state, .. } => {
                assert!(state.always_flush());
            }
            _ => panic!("Expected compressed body"),
        }
    }

    fn make_response_with_status(
        body: &'static str,
        status: http::StatusCode,
    ) -> Response<&'static str> {
        let mut response = Response::new(body);
        *response.status_mut() = status;
        response
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_compress_switching_protocols() {
        // WebSocket upgrade responses (101) must not be compressed.
        let response = make_response_with_status("", http::StatusCode::SWITCHING_PROTOCOLS);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        match wrapped.body() {
            crate::body::CompressionBody::Passthrough { .. } => {}
            _ => panic!("Expected passthrough body for 101 Switching Protocols"),
        }

        // No bogus Content-Encoding should be added to the handshake response.
        assert!(wrapped.headers().get(header::CONTENT_ENCODING).is_none());
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_compress_no_content() {
        let response = make_response_with_status("", http::StatusCode::NO_CONTENT);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        match wrapped.body() {
            crate::body::CompressionBody::Passthrough { .. } => {}
            _ => panic!("Expected passthrough body for 204 No Content"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_compress_not_modified() {
        let response = make_response_with_status("", http::StatusCode::NOT_MODIFIED);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        match wrapped.body() {
            crate::body::CompressionBody::Passthrough { .. } => {}
            _ => panic!("Expected passthrough body for 304 Not Modified"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_compress_range_response() {
        let response =
            make_response_with_headers("partial content", [("content-range", "bytes 0-99/200")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Should be passthrough for range responses
        match wrapped.body() {
            crate::body::CompressionBody::Passthrough { .. } => {}
            _ => panic!("Expected passthrough body for range response"),
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_vary_header_added() {
        let response = make_response("hello world");
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        assert_eq!(
            wrapped.headers().get(header::VARY).unwrap(),
            "accept-encoding"
        );
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_vary_header_appended() {
        let response = make_response_with_headers("hello world", [("vary", "origin")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // With append, there will be two Vary headers
        let vary_values: Vec<_> = wrapped
            .headers()
            .get_all(header::VARY)
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(vary_values, vec!["origin", "accept-encoding"]);
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_vary_header_not_duplicated() {
        let response = make_response_with_headers("hello world", [("vary", "accept-encoding")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        assert_eq!(
            wrapped.headers().get(header::VARY).unwrap(),
            "accept-encoding"
        );
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_vary_header_star_not_modified() {
        let response = make_response_with_headers("hello world", [("vary", "*")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        assert_eq!(wrapped.headers().get(header::VARY).unwrap(), "*");
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_accept_ranges_removed() {
        let response = make_response_with_headers("hello world", [("accept-ranges", "bytes")]);
        let wrapped = wrap_response(response, Some(Codec::Gzip), 0);

        // Accept-Ranges should be removed when compressing
        assert!(wrapped.headers().get(header::ACCEPT_RANGES).is_none());
    }

    #[test]
    fn test_accept_ranges_kept_when_not_compressing() {
        let response = make_response_with_headers("hello world", [("accept-ranges", "bytes")]);
        let wrapped = wrap_response(response, None, 0);

        // Accept-Ranges should be kept when not compressing
        assert_eq!(
            wrapped.headers().get(header::ACCEPT_RANGES).unwrap(),
            "bytes"
        );
    }
}
