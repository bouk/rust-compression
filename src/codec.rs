use compression_codecs::EncodeV2;
#[cfg(feature = "brotli")]
use compression_codecs::brotli::{BrotliEncoder, params::EncoderParams as BrotliParams};
#[cfg(feature = "deflate")]
use compression_codecs::deflate::DeflateEncoder;
#[cfg(feature = "gzip")]
use compression_codecs::gzip::GzipEncoder;
#[cfg(feature = "zstd")]
use compression_codecs::zstd::ZstdEncoder;
#[cfg(any(feature = "gzip", feature = "deflate"))]
use compression_core::Level;

/// Supported compression codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Codec {
    /// Zstd compression.
    #[cfg(feature = "zstd")]
    Zstd,
    /// Brotli compression.
    #[cfg(feature = "brotli")]
    Brotli,
    /// Gzip compression.
    #[cfg(feature = "gzip")]
    Gzip,
    /// Deflate compression.
    #[cfg(feature = "deflate")]
    Deflate,
}

impl Codec {
    /// Returns the Content-Encoding header value for this codec.
    pub fn content_encoding(&self) -> &'static str {
        match self {
            #[cfg(feature = "zstd")]
            Codec::Zstd => "zstd",
            #[cfg(feature = "brotli")]
            Codec::Brotli => "br",
            #[cfg(feature = "gzip")]
            Codec::Gzip => "gzip",
            #[cfg(feature = "deflate")]
            Codec::Deflate => "deflate",
        }
    }

    /// Creates a new encoder for this codec.
    pub fn encoder(&self) -> Box<dyn EncodeV2 + Send> {
        match self {
            #[cfg(feature = "zstd")]
            Codec::Zstd => Box::new(ZstdEncoder::new(3)),
            #[cfg(feature = "brotli")]
            Codec::Brotli => Box::new(BrotliEncoder::new(BrotliParams::default())),
            #[cfg(feature = "gzip")]
            Codec::Gzip => Box::new(GzipEncoder::new(Level::Default.into())),
            #[cfg(feature = "deflate")]
            Codec::Deflate => Box::new(DeflateEncoder::new(Level::Default.into())),
        }
    }

    /// Parses the Accept-Encoding header and returns the best supported codec.
    ///
    /// The header value is expected to be comma-separated encodings with optional
    /// quality values (e.g., "gzip, br;q=1.0, zstd;q=0.8").
    pub fn from_accept_encoding(header: &str) -> Option<Codec> {
        let mut best_codec: Option<(Codec, f32)> = None;

        for part in header.split(',') {
            let part = part.trim();
            let (encoding, quality) = parse_encoding_with_quality(part);

            // Skip if quality is 0
            if quality == 0.0 {
                continue;
            }

            #[allow(unused_mut)]
            let mut codec = None;
            #[cfg(feature = "zstd")]
            if encoding == "zstd" {
                codec = Some(Codec::Zstd);
            }
            #[cfg(feature = "brotli")]
            if codec.is_none() && (encoding == "br" || encoding == "brotli") {
                codec = Some(Codec::Brotli);
            }
            #[cfg(feature = "gzip")]
            if codec.is_none() && (encoding == "gzip" || encoding == "x-gzip") {
                codec = Some(Codec::Gzip);
            }
            #[cfg(feature = "deflate")]
            if codec.is_none() && encoding == "deflate" {
                codec = Some(Codec::Deflate);
            }

            if let Some(codec) = codec {
                match &best_codec {
                    None => best_codec = Some((codec, quality)),
                    Some((_, best_quality)) if quality > *best_quality => {
                        best_codec = Some((codec, quality));
                    }
                    // Prefer zstd > brotli > gzip > deflate when quality is equal
                    Some((best, best_quality))
                        if quality == *best_quality && codec.priority() < best.priority() =>
                    {
                        best_codec = Some((codec, quality));
                    }
                    _ => {}
                }
            }
        }

        best_codec.map(|(codec, _)| codec)
    }

    /// Returns the priority of this codec (lower is better).
    fn priority(&self) -> u8 {
        match self {
            #[cfg(feature = "zstd")]
            Codec::Zstd => 0,
            #[cfg(feature = "brotli")]
            Codec::Brotli => 1,
            #[cfg(feature = "gzip")]
            Codec::Gzip => 2,
            #[cfg(feature = "deflate")]
            Codec::Deflate => 3,
        }
    }
}

/// Parses an encoding entry like "gzip" or "br;q=0.8" into (encoding, quality).
fn parse_encoding_with_quality(s: &str) -> (&str, f32) {
    let mut parts = s.splitn(2, ';');
    let encoding = parts.next().unwrap_or("").trim();

    let quality = parts
        .next()
        .and_then(|q| {
            let q = q.trim();
            if q.starts_with("q=") || q.starts_with("Q=") {
                q[2..].parse::<f32>().ok()
            } else {
                None
            }
        })
        .unwrap_or(1.0);

    (encoding, quality)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_encoding() {
        #[cfg(feature = "zstd")]
        assert_eq!(Codec::Zstd.content_encoding(), "zstd");
        #[cfg(feature = "brotli")]
        assert_eq!(Codec::Brotli.content_encoding(), "br");
        #[cfg(feature = "gzip")]
        assert_eq!(Codec::Gzip.content_encoding(), "gzip");
        #[cfg(feature = "deflate")]
        assert_eq!(Codec::Deflate.content_encoding(), "deflate");
    }

    #[test]
    fn test_from_accept_encoding_simple() {
        #[cfg(feature = "zstd")]
        assert_eq!(Codec::from_accept_encoding("zstd"), Some(Codec::Zstd));
        #[cfg(feature = "brotli")]
        assert_eq!(Codec::from_accept_encoding("br"), Some(Codec::Brotli));
        #[cfg(feature = "gzip")]
        assert_eq!(Codec::from_accept_encoding("gzip"), Some(Codec::Gzip));
        #[cfg(feature = "deflate")]
        assert_eq!(Codec::from_accept_encoding("deflate"), Some(Codec::Deflate));
    }

    #[test]
    #[cfg(all(feature = "zstd", feature = "brotli", feature = "gzip"))]
    fn test_from_accept_encoding_multiple() {
        // With equal quality, prefer zstd
        assert_eq!(
            Codec::from_accept_encoding("gzip, br, zstd"),
            Some(Codec::Zstd)
        );
    }

    #[test]
    #[cfg(all(feature = "gzip", feature = "brotli"))]
    fn test_from_accept_encoding_with_quality() {
        assert_eq!(
            Codec::from_accept_encoding("gzip;q=1.0, br;q=0.5"),
            Some(Codec::Gzip)
        );
        assert_eq!(
            Codec::from_accept_encoding("gzip;q=0.5, br;q=1.0"),
            Some(Codec::Brotli)
        );
    }

    #[test]
    fn test_from_accept_encoding_unsupported() {
        assert_eq!(Codec::from_accept_encoding("identity"), None);
        assert_eq!(Codec::from_accept_encoding("compress"), None);
    }

    #[test]
    #[cfg(all(feature = "gzip", feature = "brotli"))]
    fn test_from_accept_encoding_quality_zero() {
        assert_eq!(Codec::from_accept_encoding("gzip;q=0"), None);
        assert_eq!(
            Codec::from_accept_encoding("gzip;q=0, br"),
            Some(Codec::Brotli)
        );
    }
}
