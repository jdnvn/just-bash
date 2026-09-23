import { describe, expect, it } from "vitest";
import { Bash } from "../../Bash.js";

function environment() {
  return new Bash({
    cwd: "/tree",
    files: {
      "/tree/.gitignore": "ignored/\n!visible/.hidden.ts\n",
      "/tree/ignored/file.ts": "hello\n",
      "/tree/visible/.hidden.ts": "hello\n",
      "/tree/visible/.gitignore": "skip.ts\n",
      "/tree/visible/skip.ts": "hello\n",
      "/tree/visible/a.txt": "before\nhello héllo\nafter\nHELLO\n",
      "/tree/visible/b.ts": "no match\n",
      "/tree/z.ts": "hello\n",
      "/tree/.secret": "hello\n",
      "/patterns": "hello\n",
    },
  });
}

describe("rg streaming compatibility", () => {
  for (const args of [
    "hello",
    "hello .",
    "hello ./",
    "hello visible/a.txt",
    "-n hello visible/a.txt",
    "-H hello visible/a.txt",
    "-I hello",
    "-N hello",
    "-l hello",
    "-l -0 hello",
    "--files-without-match hello",
    "--files-without-match absent",
    "--files-without-match --quiet hello",
    "--files-without-match --quiet absent",
    "-q hello",
    "-q absent",
    "-v hello",
    "-o hello",
    "-n -o hello",
    "-c hello",
    "--count-matches hello",
    "--include-zero -c hello",
    "--heading hello",
    "--passthru hello",
    "-C 1 hello",
    "--column --byte-offset hello",
    "--vimgrep hello",
    "-r goodbye hello",
    "-f /patterns",
    "--hidden hello",
    "--no-ignore hello",
    "-t ts hello",
    "-g '*.ts' hello",
    "--max-depth 1 hello",
    "--max-filesize 10 hello",
    "--sort none hello",
    "--sort path hello",
    "--files",
    "--files --hidden",
    "--files -0",
    "--files -q",
    "--files -t ts",
    "--files --sort none",
    "--files missing",
    "absent",
    "hello missing",
    "hello visible/a.txt z.ts",
    "--files visible z.ts",
    "--json hello",
    "--json -q hello",
    "--stats hello",
  ]) {
    it(`preserves buffered output and status: ${args}`, async () => {
      const expected = await environment().exec(`rg ${args}`);
      const actual = await environment().exec(
        `set -o pipefail; rg ${args} | cat`,
      );
      expect(actual.stdout).toBe(expected.stdout);
      expect(actual.stderr).toBe(expected.stderr);
      expect(actual.exitCode).toBe(expected.exitCode);
    });
  }

  it("reads UTF-8 stdin and stdin pattern files through the pipe", async () => {
    const bash = environment();
    const stdin = await bash.exec("printf 'héllo\nother\n' | rg héllo | cat");
    expect(stdin.stdout).toBe("1:héllo\n");
    expect(stdin.stderr).toBe("");
    expect(stdin.exitCode).toBe(0);
    const pattern = await bash.exec(
      "printf 'héllo\n' | rg -f - -f - visible/a.txt | cat",
    );
    expect(pattern.stdout).toBe("hello héllo\n");
    expect(pattern.stderr).toBe("");
    expect(pattern.exitCode).toBe(0);
  });

  it("preserves sorted symlink traversal and terminates cycles", async () => {
    const bash = environment();
    await bash.fs.symlink("/tree/visible", "/tree/a");
    await bash.fs.symlink("/tree", "/tree/visible/loop");
    await bash.fs.writeFile("/tree/a.txt", "hello\n");
    const expected = await bash.exec("rg -L --files");
    const actual = await bash.exec("rg -L --files | cat");
    expect(actual.stdout).toBe(expected.stdout);
    expect(actual.stderr).toBe(expected.stderr);
    expect(actual.exitCode).toBe(expected.exitCode);
  });
});
