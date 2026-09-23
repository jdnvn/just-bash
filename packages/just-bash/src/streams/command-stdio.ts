import {
  type ByteString,
  latin1FromBytes,
  unsafeBytesFromLatin1,
} from "../encoding.js";
import { ExecutionLimitError } from "../interpreter/errors.js";
import type { RuntimeCommandContext } from "../types.js";

export interface CommandStdio {
  read(): Promise<ByteString | null>;
  write(chunk: ByteString): Promise<void>;
}

export async function readCommandStdin(
  ctx: RuntimeCommandContext,
): Promise<ByteString> {
  if (!ctx.stdio) return ctx.stdin;
  const limit = Math.min(ctx.limits.maxInputBytes, ctx.limits.maxStringLength);
  let content = "";
  for (;;) {
    const chunk = await ctx.stdio.read();
    if (chunk === null) return unsafeBytesFromLatin1(content);
    const value = latin1FromBytes(chunk);
    if (value.length > limit - content.length) {
      throw new ExecutionLimitError(
        `pipeline: buffered input size limit exceeded (${limit} bytes)`,
        "string_length",
      );
    }
    content += value;
  }
}
