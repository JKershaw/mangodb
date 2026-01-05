/**
 * Simple in-process mutex for serializing async operations.
 */
export class Mutex {
  private queue = Promise.resolve();

  async runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    let resolve: () => void;
    const tail = this.queue;
    this.queue = new Promise((r) => (resolve = r));

    await tail;
    try {
      return await fn();
    } finally {
      resolve!();
    }
  }
}
