const std = @import("std");
const pipeline = @import("pipeline.zig");

/// nextFn may be one of the following:
/// - fn (Context) ?T
/// - fn (*Context) ?T
/// - fn (*const Context) ?T
/// - fn (Context) !?T
/// - fn (*Context) !?T
/// - fn (*const Context) !?T
pub fn Stream(
    comptime T: type,
    comptime Context: type,
    comptime nextFn: anytype,
) type {
    return struct {
        context: Context,

        const Self = @This();

        pub usingnamespace blk: {
            const fi = @typeInfo(@TypeOf(nextFn)).Fn;
            const Ctx = fi.args[0].arg_type.?;
            const SelfI = if (Ctx == Context)
                Self
            else if (Ctx == *const Context)
                *const Self
            else
                *Self;
            const Ret = fi.return_type.?;

            break :blk StreamInternal(SelfI, T, Ret, nextFn);
        };
    };
}

fn StreamInternal(
    comptime Self: type,
    comptime Child: type,
    comptime NextRet: type,
    comptime nextFn: anytype,
) type {
    const ptr = @typeInfo(Self) == .Pointer;
    return struct {
        pub fn next(self: Self) NextRet {
            return if (ptr)
                nextFn(&self.context)
            else
                nextFn(self.context);
        }

        pub fn map(self: Self, pipe: anytype) Map(@TypeOf(pipe)) {
            return Map(@TypeOf(pipe)){ .context = .{
                .stream = self,
                .pipe = pipe,
            } };
        }
        pub fn Map(comptime Pipe: type) type {
            if (Pipe.pipeline_signature.In != Child) {
                @compileError("Expected pipeline 'Pipeline(" ++
                    @typeName(Child) ++ " -> *)', got '" ++
                    Pipe.pipeline_name ++ "'");
            }
            const Out = Pipe.pipeline_signature.Out;
            const M = MapStream(Self, Pipe);
            return Stream(Out, M, M.next);
        }

        pub fn split(self: Self, pipe: anytype) Split(@TypeOf(pipe)) {
            return Split(@TypeOf(pipe)){ .context = .{
                .stream = self,
                .pipe = pipe,
            } };
        }
        pub fn Split(comptime Pipe: type) type {
            if (Pipe.pipeline_signature.In != Child or Pipe.pipeline_signature.Out != bool) {
                @compileError("Expected pipeline 'Pipeline(" ++
                    @typeName(Child) ++ " -> bool)', got '" ++
                    Pipe.pipeline_name ++ "'");
            }
            const S = SplitStream(Self, Pipe);
            return Stream(S.Result, S, S.next);
        }
    };
}

fn MapStream(comptime S: type, comptime P: type) type {
    return struct {
        stream: S,
        pipe: P,

        const Self = @This();
        fn next(self: Self) ?P.pipeline_signature.Out {
            const v = self.stream.next() orelse return null;
            return self.pipe.apply(v);
        }
    };
}

pub fn SplitStream(comptime S: type, comptime P: type) type {
    return struct {
        stream: S,
        pipe: P,
        state: enum {
            delim,
            child,
            done,
        } = .delim,

        const Self = @This();
        fn next(self: *Self) ?Result {
            switch (self.state) {
                .delim => {
                    self.state = .child;
                    return Result{ .context = self };
                },
                .done => return null,

                .child => unreachable,
            }
        }
        fn resultNext(self: *Self) ?P.pipeline_signature.In {
            std.debug.assert(self.state == .child);
            const v = self.stream.next() orelse {
                self.state = .done;
                return null;
            };
            if (self.pipe.apply(v)) {
                self.state = .delim;
                return null;
            }
            return v;
        }

        pub const Result = Stream(P.pipeline_signature.In, *Self, resultNext);
    };
}

pub const constructors = struct {
    pub fn sliceStream(comptime T: type, slice: []const T) Stream(T, SliceIterator(T), SliceIterator(T).next) {
        return .{ .context = .{ .slice = slice } };
    }
};

pub fn SliceIterator(comptime T: type) type {
    return struct {
        index: usize = 0,
        slice: []const T,

        const Self = @This();
        pub fn next(self: *Self) ?T {
            if (self.index >= self.slice.len) return null;
            const v = self.slice[self.index];
            self.index += 1;
            return v;
        }
    };
}

test "sliceStream" {
    const xs = [_]u32{
        1, 2, 3, 4, 5, 6, 7, 8, 9,
        9, 8, 7, 6, 5, 4, 3, 2, 1,
    };

    var s = constructors.sliceStream(u32, &xs);

    var i: ?u32 = 1;
    while (i.? < 10) : (i.? += 1) {
        try std.testing.expectEqual(i, s.next());
    }
    while (i.? > 1) {
        i.? -= 1;
        try std.testing.expectEqual(i, s.next());
    }
    i = null;
    try std.testing.expectEqual(i, s.next());
}

test "Stream.map" {
    const xs = [_]u32{ 0, 2, 4, 6, 8, 10 };
    var s0 = constructors.sliceStream(u32, &xs);

    const S = struct {
        fn div2(x: u32) u32 {
            return x / 2;
        }
    };
    const P = pipeline.Pipeline(u32, u32, &.{
        pipeline.Atom.init(S.div2),
    });
    const s1 = s0.map(P.init(.{{}}));

    var i: ?u32 = 0;
    while (i.? < 6) : (i.? += 1) {
        try std.testing.expectEqual(i, s1.next());
    }
    i = null;
    try std.testing.expectEqual(i, s1.next());
}

test "Stream.split" {
    const xs = [_]u32{ 1, 2, 3, 0, 4, 5, 6, 0, 7, 8, 9 };
    var s0 = constructors.sliceStream(u32, &xs);

    var s1 = s0.split(pipeline.constructors.eql(u32, 0));

    var s = s1.next() orelse return error.TestExpectedNotNull;
    try std.testing.expectEqual(@as(?u32, 1), s.next());
    try std.testing.expectEqual(@as(?u32, 2), s.next());
    try std.testing.expectEqual(@as(?u32, 3), s.next());
    try std.testing.expectEqual(@as(?u32, null), s.next());

    s = s1.next() orelse return error.TestExpectedNotNull;
    try std.testing.expectEqual(@as(?u32, 4), s.next());
    try std.testing.expectEqual(@as(?u32, 5), s.next());
    try std.testing.expectEqual(@as(?u32, 6), s.next());
    try std.testing.expectEqual(@as(?u32, null), s.next());

    s = s1.next() orelse return error.TestExpectedNotNull;
    try std.testing.expectEqual(@as(?u32, 7), s.next());
    try std.testing.expectEqual(@as(?u32, 8), s.next());
    try std.testing.expectEqual(@as(?u32, 9), s.next());
    try std.testing.expectEqual(@as(?u32, null), s.next());
}
