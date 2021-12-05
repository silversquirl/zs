//! Composable stream/pipeline library
//! Designed primarily for working with text, but can work on other stuff too

pub const stream = @import("stream.zig");
pub const pipeline = @import("pipeline.zig");

pub usingnamespace stream.constructors;
pub usingnamespace pipeline.constructors;

test {
    _ = pipeline;
    _ = stream;
}
