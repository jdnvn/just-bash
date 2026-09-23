import { afterEach, beforeEach, describe, it } from "vitest";
import {
  cleanupTestDir,
  compareOutputs,
  createTestDir,
  setupFiles,
} from "./fixture-runner.js";

describe("rg streaming - Real Bash Comparison", () => {
  let testDir: string;
  beforeEach(async () => {
    testDir = await createTestDir();
  });
  afterEach(async () => {
    await cleanupTestDir(testDir);
  });

  for (const command of [
    "rg --sort path --files --hidden tree | head -4",
    "rg --sort path -l pattern tree | head -2",
    "rg --sort path -n pattern tree | head -3",
    "rg --sort path -n -I pattern tree | cat",
    "rg --sort path -c pattern tree | cat",
    "rg --sort path --files-without-match pattern tree | cat",
    "rg --sort path --files -0 tree | cat",
    "rg --sort path -n -C 1 pattern tree/a.txt | cat",
    "rg pattern tree/a.txt | cat",
    "printf 'pattern\n' | rg -f - tree/a.txt | cat",
  ]) {
    it(command, async () => {
      const bash = await setupFiles(testDir, {
        "tree/.hidden.txt": "pattern\n",
        "tree/a.txt": "before\npattern héllo\nafter\n",
        "tree/d/b.txt": "pattern\n",
        "tree/b.txt": "no match\n",
        "tree/c.txt": "pattern\n",
      });
      await compareOutputs(bash, testDir, command);
    });
  }
});
