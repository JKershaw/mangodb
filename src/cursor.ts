type Document = Record<string, unknown>;

/**
 * MongoneCursor represents a cursor over query results.
 * It mirrors the Cursor API from the official MongoDB driver.
 */
export class MongoneCursor<T extends Document = Document> {
  private readonly fetchDocuments: () => Promise<T[]>;

  constructor(fetchDocuments: () => Promise<T[]>) {
    this.fetchDocuments = fetchDocuments;
  }

  /**
   * Return all documents as an array.
   */
  async toArray(): Promise<T[]> {
    return this.fetchDocuments();
  }
}
