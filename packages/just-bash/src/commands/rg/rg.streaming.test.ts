import { describe, expect, it } from "vitest";
import { Bash } from "../../Bash.js";
import { InMemoryFs } from "../../fs/in-memory-fs/index.js";

class CountingFs extends InMemoryFs {
  directories: string[] = [];
  reads: string[] = [];

  override async readdirWithFileTypes(path: string) {
    this.directories.push(path);
    return super.readdirWithFileTypes(path);
  }

  override async readFileBuffer(path: string) {
    if (path.endsWith(".txt")) this.reads.push(path);
    return super.readFileBuffer(path);
  }
}

function tree() {
  const files: Record<string, string> = Object.create(null);
  for (let directory = 0; directory < 50; directory++) {
    for (let file = 0; file < 10; file++) {
      files[`/mnt/Home/${String(directory).padStart(2, "0")}/${file}.txt`] =
        "pattern\n";
    }
  }
  return new CountingFs(files);
}

describe("rg streaming pipelines", () => {
  it("stops discovering files when head has four paths", async () => {
    const fs = tree();
    const result = await new Bash({ fs }).exec(
      "rg --files --hidden /mnt/Home | head -4",
    );
    expect(result.stdout).toBe(
      "/mnt/Home/00/0.txt\n/mnt/Home/00/1.txt\n/mnt/Home/00/2.txt\n/mnt/Home/00/3.txt\n",
    );
    expect(result.stderr).toBe("");
    expect(result.exitCode).toBe(0);
    expect(fs.directories).toEqual(["/mnt/Home", "/mnt/Home/00"]);
    expect(fs.reads).toEqual([]);
  });

  it("stops searching files when head has 200 matching paths", async () => {
    const fs = tree();
    const result = await new Bash({ fs }).exec(
      "rg -l pattern /mnt/Home | head -200",
    );
    const expected = [];
    for (let directory = 0; directory < 20; directory++) {
      for (let file = 0; file < 10; file++) {
        expected.push(
          `/mnt/Home/${String(directory).padStart(2, "0")}/${file}.txt`,
        );
      }
    }
    expect(result.stdout).toBe(`${expected.join("\n")}\n`);
    expect(result.stderr).toBe("");
    expect(result.exitCode).toBe(0);
    expect(fs.reads.length).toBeGreaterThanOrEqual(200);
    expect(fs.reads.length).toBeLessThanOrEqual(201);
    expect(fs.directories.length).toBeLessThanOrEqual(22);
  });

  it("sorts directory prefixes by the complete path", async () => {
    const bash = new Bash({
      cwd: "/tree",
      files: {
        "/tree/a/z.txt": "pattern\n",
        "/tree/a.txt": "pattern\n",
        "/tree/a-/z.txt": "pattern\n",
        "/tree/a0.txt": "pattern\n",
        "/tree/a/b/z.txt": "pattern\n",
        "/tree/a/b.txt": "pattern\n",
      },
    });
    const result = await bash.exec("rg --files | cat");
    expect(result.stdout).toBe(
      "a-/z.txt\na.txt\na/b.txt\na/b/z.txt\na/z.txt\na0.txt\n",
    );
    expect(result.stderr).toBe("");
    expect(result.exitCode).toBe(0);
  });

  it("propagates broken pipe through traversal to PIPESTATUS", async () => {
    const result = await new Bash({ fs: tree() }).exec(
      "set -o pipefail; rg --files /mnt/Home | head -4; echo $?:${PIPESTATUS[*]}",
    );
    expect(result.stdout).toBe(
      "/mnt/Home/00/0.txt\n/mnt/Home/00/1.txt\n/mnt/Home/00/2.txt\n/mnt/Home/00/3.txt\n141:141 0\n",
    );
    expect(result.stderr).toBe("");
    expect(result.exitCode).toBe(0);
  });

  it("stops quiet searches after the first matching file", async () => {
    const fs = tree();
    const result = await new Bash({ fs }).exec(
      "set -o pipefail; rg -q pattern /mnt/Home | cat",
    );
    expect(result.stdout).toBe("");
    expect(result.stderr).toBe("");
    expect(result.exitCode).toBe(0);
    expect(fs.reads).toEqual(["/mnt/Home/00/0.txt"]);
    expect(fs.directories).toEqual(["/mnt/Home", "/mnt/Home/00"]);
  });

  it("retains traversal limits while consuming a complete stream", async () => {
    const result = await new Bash({
      fs: tree(),
      executionLimits: { maxTraversalEntries: 8 },
    }).exec("rg --files /mnt/Home | cat");
    expect(result.exitCode).toBe(126);
    expect(result.stderr).toMatch(/filesystem traversal entry limit exceeded/);
  });

  it("retains aggregate file input limits", async () => {
    const result = await new Bash({
      fs: tree(),
      executionLimits: { maxInputBytes: 16 },
    }).exec("rg -l pattern /mnt/Home | cat");
    expect(result.exitCode).toBe(126);
    expect(result.stderr).toMatch(/input.*limit exceeded/);
  });
});
