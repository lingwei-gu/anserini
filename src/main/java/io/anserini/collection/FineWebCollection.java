/*
 * Anserini: A Lucene toolkit for reproducible information retrieval research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.anserini.collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A FineWeb document collection stored in Parquet format.
 * This class reads all <code>.parquet</code> files in the input directory.
 * Each Parquet file contains multiple documents with fields such as:
 * <ul>
 *   <li><code>id</code>: unique document identifier (required)</li>
 *   <li><code>text</code> or <code>contents</code>: document text content (required)</li>
 *   <li>Additional metadata fields: url, domain, etc. (optional)</li>
 * </ul>
 */
public class FineWebCollection extends DocumentCollection<FineWebCollection.Document> {
  private static final Logger LOG = LogManager.getLogger(FineWebCollection.class);
  
  protected String idField = "id";
  protected String contentsField = null; // Will auto-detect "text" or "contents"

  public FineWebCollection() {
  }

  public FineWebCollection(Path path) {
    this.path = path;
    this.allowedFileSuffix = new HashSet<>(Arrays.asList(".parquet"));
  }

  public FineWebCollection(Path path, String idField, String contentsField) {
    this.path = path;
    this.allowedFileSuffix = new HashSet<>(Arrays.asList(".parquet"));
    this.idField = idField;
    this.contentsField = contentsField;
  }

  @Override
  public FileSegment<FineWebCollection.Document> createFileSegment(Path p) throws IOException {
    return new Segment(p, idField, contentsField);
  }

  @Override
  public FileSegment<FineWebCollection.Document> createFileSegment(BufferedReader bufferedReader) throws IOException {
    throw new UnsupportedOperationException("BufferedReader is not supported for FineWebCollection. Use Path-based constructor.");
  }

  /**
   * A file in a FineWeb collection, typically containing multiple documents.
   */
  public static class Segment extends FileSegment<FineWebCollection.Document> {
    private ParquetReader<Group> reader;
    private boolean readerInitialized;
    private String idField;
    private String contentsField;
    // Counters and buffers to support splitting each Parquet row into multiple paragraph-level documents.
    private long rowCounter = 0; // Counts Parquet records (rows) within this segment.
    private List<String> paragraphs = new ArrayList<>();
    private int paragraphIndex = 0;

    public Segment(Path path) throws IOException {
      this(path, "id", null);
    }

    public Segment(Path path, String idField, String contentsField) throws IOException {
      super(path);
      this.idField = idField;
      this.contentsField = contentsField;
      this.rowCounter = 0;
      initializeParquetReader(path);
    }

    public Segment(BufferedReader bufferedReader) throws IOException {
      super(bufferedReader);
      throw new IOException("BufferedReader constructor not supported for Parquet files");
    }

    /**
     * Initializes the Parquet reader.
     *
     * @param path the path to the Parquet file.
     * @throws IOException if an I/O error occurs during file reading.
     */
    private void initializeParquetReader(Path path) throws IOException {
      org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toString());
      reader = ParquetReader.builder(new GroupReadSupport(), hadoopPath).build();
      readerInitialized = true;
    }

    @Override
    public void readNext() throws NoSuchElementException {
      if (atEOF || !readerInitialized) {
        throw new NoSuchElementException("End of file reached");
      }

      try {
        // If we still have remaining paragraphs from the current row, emit the next one.
        if (paragraphIndex < paragraphs.size()) {
          String paragraph = paragraphs.get(paragraphIndex++);
          bufferedRecord = createNewDocument(paragraph, getSegmentPath(), rowCounter - 1, paragraphIndex - 1);
          return;
        }

        // Otherwise, read the next Parquet record and split its text into paragraphs.
        while (true) {
          Group record = reader.read();
          if (record == null) {
            atEOF = true;
            if (reader != null) {
              reader.close();
            }
            readerInitialized = false;
            throw new NoSuchElementException("End of file reached");
          }

          long currentRow = rowCounter++;

          String text = null;
          try {
            // Use contentsField if provided, otherwise default to "text".
            String fieldName = contentsField != null ? contentsField : "text";
            text = record.getString(fieldName, 0);
          } catch (RuntimeException e) {
            LOG.warn("Skipping row {} due to missing or invalid contents field: {}", currentRow, e.getMessage());
            continue; // Try next record.
          }

          paragraphs = splitIntoParagraphs(text);
          paragraphIndex = 0;

          if (paragraphs.isEmpty()) {
            LOG.debug("Row {} produced no non-empty paragraphs, skipping.", currentRow);
            continue;
          }

          String paragraph = paragraphs.get(paragraphIndex++);
          bufferedRecord = createNewDocument(paragraph, getSegmentPath(), currentRow, paragraphIndex - 1);
          return;
        }
      } catch (IOException e) {
        LOG.error("Error reading Parquet record", e);
        throw new NoSuchElementException("Error reading Parquet record: " + e.getMessage());
      }
    }

    /**
     * Split a block of text into paragraphs. We treat one or more blank lines as a paragraph
     * separator. Single-line texts will become a single paragraph.
     */
    private List<String> splitIntoParagraphs(String text) {
      List<String> result = new ArrayList<>();
      if (text == null) {
        return result;
      }

      // Split on one or more blank lines.
      String[] rawParas = text.split("\\n");
      for (String p : rawParas) {
        String trimmed = p.trim();
        if (!trimmed.isEmpty()) {
          result.add(trimmed);
        }
      }

      // Fallback: if splitting somehow produced nothing but the original text is non-empty,
      // keep the whole text as a single paragraph.
      if (result.isEmpty()) {
        String trimmed = text.trim();
        if (!trimmed.isEmpty()) {
          result.add(trimmed);
        }
      }

      return result;
    }

    protected Document createNewDocument(String paragraph, Path segmentPath, long rowNumber, int paragraphNumber) {
      String segmentName = segmentPath != null ? segmentPath.getFileName().toString().replace(".parquet", "") : "doc";
      String id = segmentName + "_" + rowNumber + "_" + paragraphNumber;
      return new Document(id, paragraph);
    }
  }

  /**
   * A document in a FineWeb collection.
   */
  public static class Document extends MultifieldSourceDocument {
    private String id;
    private String contents;
    private String raw;
    private Map<String, String> fields;

    public Document(String id, String contents) {
      this.fields = new HashMap<>();
      this.id = id;
      this.contents = contents;
      // For FineWeb, it's sufficient (and often desirable) for raw to just be the paragraph text.
      this.raw = contents;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public String contents() {
      if (contents == null) {
        throw new RuntimeException("FineWeb document has no \"contents\" or \"text\" field!");
      }
      return contents;
    }

    @Override
    public String raw() {
      return raw;
    }

    @Override
    public boolean indexable() {
      return true;
    }

    @Override
    public Map<String, String> fields() {
      return fields;
    }
  }
}

