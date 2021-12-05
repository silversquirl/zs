const std = @import("std");

// TODO: handle error unions better
pub fn Pipeline(comptime In: type, comptime Out: type, atoms: []const Atom) type {
    comptime { // Type check atoms
        var T = In;
        for (atoms) |atom| {
            if (atom.In != T) {
                @compileError("Expected atom with '" ++ @typeName(T) ++
                    "' input, got '" ++ @typeName(atom.In) ++ "'");
            }
            T = atom.Out;
        }
        if (T != Out) {
            @compileError("Expected atom with '" ++ @typeName(Out) ++
                "' output, got '" ++ @typeName(T) ++ "'");
        }
    }

    return struct {
        pub const pipeline_signature = PipelineSig{ .In = In, .Out = Out };
        pub const pipeline_name = "zs.Pipeline(" ++
            @typeName(pipeline_signature.In) ++ " -> " ++
            @typeName(pipeline_signature.Out) ++ ")";

        const Self = @This();

        pub const Contexts = blk: {
            var tys: [atoms.len]type = undefined;
            for (atoms) |atom, i| {
                tys[i] = atom.Context;
            }
            break :blk std.meta.Tuple(&tys);
        };
        const Outputs = blk: {
            var tys: [atoms.len]type = undefined;
            for (atoms) |atom, i| {
                tys[i] = atom.Out;
            }
            break :blk std.meta.Tuple(&tys);
        };

        contexts: Contexts,

        /// Create a new pipeline with the specified context values
        pub fn init(contexts: Contexts) Self {
            return .{ .contexts = contexts };
        }

        /// Apply a pipeline to a single value
        pub fn apply(self: Self, in: In) Out {
            var outs: Outputs = undefined;
            inline for (atoms) |atom, i| {
                const ain = if (i == 0) in else outs[i - 1];
                if (atom.Context == void) {
                    outs[i] = atom.func(ain);
                } else {
                    outs[i] = atom.func(self.contexts[i], ain);
                }
            }
            return outs[outs.len - 1];
        }
    };
}
const PipelineSig = struct {
    In: type,
    Out: type,
};

pub const Atom = struct {
    In: type,
    Out: type,
    Context: type,
    func: anytype,

    pub fn init(func: anytype) Atom {
        // TODO: type check more thoroughly and produce nicer errors
        const fi = @typeInfo(@TypeOf(func)).Fn;
        return .{
            .In = fi.args[fi.args.len - 1].arg_type.?,
            .Out = fi.return_type.?,
            .Context = if (fi.args.len == 1) void else fi.args[0].arg_type.?,
            .func = func,
        };
    }
};

pub const constructors = struct {
    pub fn eql(comptime T: type, value: T) Eql(T) {
        return Eql(T).init(.{value});
    }

    pub fn map(comptime Stream: type, pipe: anytype) Map(Stream, @TypeOf(pipe)) {
        return Map(Stream, @TypeOf(pipe)).init(.{pipe});
    }
};

pub fn Eql(comptime T: type) type {
    const S = struct {
        fn atom(a: T, b: T) bool {
            return a == b;
        }
    };
    return Pipeline(T, bool, &.{
        Atom.init(S.atom),
    });
}

pub fn Map(comptime Stream: type, comptime Pipe: type) type {
    const S = struct {
        fn atom(p: Pipe, s: Stream) Stream.Map(Pipe) {
            return s.map(p);
        }
    };
    return Pipeline(Stream, Stream.Map(Pipe), &.{
        Atom.init(S.atom),
    });
}

test "Pipeline" {
    const S = struct {
        fn add(a: u32, b: u32) u32 {
            return a + b;
        }
        fn mul(a: u32, b: u32) u32 {
            return a * b;
        }
    };

    const Pipe = Pipeline(u32, u32, &.{
        Atom.init(S.add),
        Atom.init(S.add),
        Atom.init(S.mul),
        Atom.init(S.add),
    });

    // (x + 1 + 7) * 3 + 2
    const pipe = Pipe.init(.{
        1, 7,
        3, 2,
    });

    try std.testing.expectEqual(@as(u32, 56), pipe.apply(10));

    var x: u32 = 0;
    while (x < 10_000) : (x += 1) {
        try std.testing.expectEqual((x + 1 + 7) * 3 + 2, pipe.apply(x));
    }
}
