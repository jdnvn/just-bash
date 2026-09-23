import { combineAbortSignals } from "../abort-signals.js";
import type {
  PipelineNode,
  SimpleCommandNode,
  WordPart,
} from "../ast/types.js";
import {
  type ByteString,
  latin1FromBytes,
  stdoutAsBytes,
} from "../encoding.js";
import { ExecutionOutputAccumulator } from "../execution-output.js";
import {
  type ResourceLease,
  relinquishPipelineOutput,
} from "../execution-scope.js";
import { BrokenPipeError, BytePipe } from "../streams/byte-pipe.js";
import type { CommandStdio } from "../streams/command-stdio.js";
import type { ExecResult } from "../types.js";
import { resolveCommand } from "./command-resolution.js";
import { ExecutionAbortedError, ExecutionLimitError } from "./errors.js";
import { SHELL_BUILTINS } from "./helpers/shell-constants.js";
import { beginIsolatedShellState } from "./state-transaction.js";
import type { InterpreterContext, InterpreterState } from "./types.js";

function isStaticPart(part: WordPart): boolean {
  return (
    part.type === "Literal" ||
    part.type === "SingleQuoted" ||
    part.type === "Escaped" ||
    (part.type === "DoubleQuoted" && part.parts.every(isStaticPart))
  );
}

export interface StreamingPipelineResult {
  result: ExecResult;
  statuses: number[];
}

export async function executeStreamingPipeline(
  ctx: InterpreterContext,
  node: PipelineNode,
  execute: (
    node: SimpleCommandNode,
    state: InterpreterState,
    stdin: string,
    stdio?: CommandStdio,
  ) => Promise<ExecResult>,
): Promise<StreamingPipelineResult | undefined> {
  if (
    node.commands.length < 2 ||
    node.pipeStderr?.some(Boolean) ||
    ctx.state.shoptOptions.lastpipe ||
    ctx.state.shoptOptions.expand_aliases ||
    ctx.state.groupStdin !== undefined ||
    ctx.state.fileDescriptors?.size ||
    ctx.state.closedStandardFds?.size ||
    ctx.state.extraArgs
  )
    return;

  const stages: Array<{
    node: SimpleCommandNode;
    streaming: boolean;
    extension: boolean;
  }> = [];
  for (const command of node.commands) {
    if (
      command.type !== "SimpleCommand" ||
      command.redirections.length ||
      command.assignments.length ||
      command.name?.parts.length !== 1 ||
      command.name.parts[0].type !== "Literal" ||
      !command.args.every((arg) => arg.parts.every(isStaticPart))
    )
      return;
    const name = command.name.parts[0].value;
    if (SHELL_BUILTINS.has(name) || ctx.state.functions.has(name)) return;
    const resolved = await resolveCommand(ctx, name);
    if (!resolved || !("cmd" in resolved)) return;
    if (resolved.cmd.internalIsExtension && !resolved.cmd.streaming) return;
    stages.push({
      node: command,
      streaming: resolved.cmd.streaming === true,
      extension: resolved.cmd.internalIsExtension === true,
    });
  }
  if (!stages.some((stage) => stage.streaming)) return;

  const abort = new AbortController();
  const combined = combineAbortSignals(ctx.state.signal, abort.signal);
  const pipes = stages.slice(1).map(() => new BytePipe(ctx.executionScope));
  const output = new ExecutionOutputAccumulator(ctx.executionScope, "pipeline");
  const cancel = (error: unknown) => {
    for (const pipe of pipes) pipe.cancel(error);
  };
  const onAbort = () =>
    cancel(new ExecutionAbortedError("", "bash: execution aborted\n"));
  combined.signal?.addEventListener("abort", onAbort, { once: true });
  if (combined.signal?.aborted) onAbort();
  let failure: unknown;
  let failed = false;

  try {
    const results = await Promise.all(
      stages.map(async (stage, index): Promise<ExecResult> => {
        const state = { ...ctx.state };
        beginIsolatedShellState(state);
        state.lastArg = "";
        state.signal = combined.signal;
        const input = pipes[index - 1];
        const next = pipes[index];
        let inputLease: ResourceLease | undefined;
        let inputBytes = 0;
        const stdio: CommandStdio = {
          read: async () => {
            const chunk = input ? await input.read() : null;
            if (chunk !== null) {
              inputBytes += latin1FromBytes(chunk).length;
              if (inputBytes > ctx.limits.maxInputBytes) {
                throw new ExecutionLimitError(
                  "pipeline: input size limit exceeded",
                  "string_length",
                );
              }
            }
            return chunk;
          },
          write: async (chunk: ByteString) => {
            ctx.executionScope.consumeWork(1, "pipeline");
            if (latin1FromBytes(chunk).length > ctx.limits.maxStringLength) {
              throw new ExecutionLimitError(
                "pipeline: chunk size limit exceeded",
                "string_length",
              );
            }
            if (next) await next.write(chunk);
            else output.append("stdout", latin1FromBytes(chunk), 0, "bytes");
          },
        };
        try {
          state.commandCount = ctx.executionScope.chargeCommand();
          let stdin = "";
          if (!stage.streaming && input) {
            const limit = Math.min(
              ctx.limits.maxStringLength,
              ctx.limits.maxInputBytes,
              ctx.limits.maxOutputSize,
            );
            for (;;) {
              const chunk = await stdio.read();
              if (chunk === null) break;
              const value = latin1FromBytes(chunk);
              if (value.length > limit - stdin.length) {
                throw new ExecutionLimitError(
                  "pipeline: buffered input size limit exceeded",
                  "string_length",
                );
              }
              inputLease?.release();
              inputLease = ctx.executionScope.reserveBytes(
                "pipeline input",
                stdin.length + value.length,
                "pipeline",
              );
              stdin += value;
            }
          }
          const rawResult = await execute(
            stage.node,
            state,
            stdin,
            stage.streaming ? stdio : undefined,
          );
          const result = ctx.executionScope.accountResult(
            rawResult,
            "pipeline",
            stage.extension ? 0 : undefined,
          );
          output.append(
            "stderr",
            result.stderr,
            result.internalOutputAccounting?.stderr ?? 0,
          );
          if (result.stdout) {
            relinquishPipelineOutput(
              ctx.executionScope,
              result.internalOutputAccounting?.stdout ?? 0,
              "pipeline",
            );
            await stdio.write(stdoutAsBytes(result));
          }
          return { stdout: "", stderr: "", exitCode: result.exitCode };
        } catch (error) {
          if (error instanceof BrokenPipeError)
            return { stdout: "", stderr: "", exitCode: 141 };
          if (!failed) {
            failure = error;
            failed = true;
          }
          cancel(error);
          abort.abort();
          return { stdout: "", stderr: "", exitCode: 1 };
        } finally {
          input?.cancel();
          next?.end();
          inputLease?.release();
        }
      }),
    );
    if (failed) {
      output.prependTo(failure);
      throw failure;
    }
    const statuses = results.map((result) => result.exitCode);
    return {
      result: output.build(statuses[statuses.length - 1], {
        stdoutKind: "bytes",
      }),
      statuses,
    };
  } finally {
    cancel(new BrokenPipeError());
    combined.signal?.removeEventListener("abort", onAbort);
    combined.cleanup();
  }
}
