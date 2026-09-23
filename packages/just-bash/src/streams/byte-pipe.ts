import {
  type ByteString,
  latin1FromBytes,
  unsafeBytesFromLatin1,
} from "../encoding.js";
import type { ExecutionScope, ResourceLease } from "../execution-scope.js";

export class BrokenPipeError extends Error {
  constructor() {
    super("Broken pipe");
  }
}

export class BytePipe {
  private chunk: ByteString | null = null;
  private lease?: ResourceLease;
  private ended = false;
  private failure: unknown;
  private failed = false;
  private writing = false;
  private reading = false;
  private wakeReader?: () => void;
  private wakeWriter?: () => void;

  constructor(
    private readonly scope: ExecutionScope,
    private readonly capacity: number = 64 * 1024,
  ) {
    if (!Number.isSafeInteger(capacity) || capacity < 1)
      throw new RangeError("Invalid pipe capacity");
  }

  async write(bytes: ByteString): Promise<void> {
    if (this.writing) throw new Error("Pipe writes must be awaited");
    this.writing = true;
    try {
      this.assertWritable();
      const value = latin1FromBytes(bytes);
      for (let offset = 0; offset < value.length; offset += this.capacity) {
        this.assertWritable();
        const part = value.slice(offset, offset + this.capacity);
        this.lease = this.scope.reserveBytes(
          "pipeline",
          part.length,
          "pipeline",
        );
        this.chunk = unsafeBytesFromLatin1(part);
        const consumed = new Promise<void>((resolve, reject) => {
          this.wakeWriter = () => {
            if (this.failed) reject(this.failure);
            else resolve();
          };
        });
        this.wakeReader?.();
        this.wakeReader = undefined;
        await consumed;
      }
    } finally {
      this.writing = false;
    }
  }

  async read(): Promise<ByteString | null> {
    if (this.reading) throw new Error("Pipe reads must be awaited");
    this.reading = true;
    try {
      while (this.chunk === null && !this.ended && !this.failed) {
        await new Promise<void>((resolve) => {
          this.wakeReader = resolve;
        });
      }
      if (this.failed) throw this.failure;
      const chunk = this.chunk;
      this.chunk = null;
      this.lease?.release();
      this.lease = undefined;
      this.wakeWriter?.();
      this.wakeWriter = undefined;
      return chunk;
    } finally {
      this.reading = false;
    }
  }

  end(): void {
    this.ended = true;
    this.wakeReader?.();
    this.wakeReader = undefined;
  }

  cancel(error: unknown = new BrokenPipeError()): void {
    if (this.failed) return;
    this.failed = true;
    this.failure = error;
    this.chunk = null;
    this.lease?.release();
    this.lease = undefined;
    this.wakeReader?.();
    this.wakeWriter?.();
    this.wakeReader = undefined;
    this.wakeWriter = undefined;
  }

  private assertWritable(): void {
    if (this.failed) throw this.failure;
    if (this.ended) throw new BrokenPipeError();
  }
}
