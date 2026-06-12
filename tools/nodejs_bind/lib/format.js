/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

/**
 * Parse an individual entry value to a display string.
 *
 * @param {*} entry - The value to parse (could be object, array, or primitive).
 * @returns {string} The formatted string representation.
 */
function parseEntry(entry) {
  if (entry === null || entry === undefined) {
    return 'null';
  }
  if (typeof entry === 'object' && !Array.isArray(entry) && !(entry instanceof Date)) {
    // Object: vertex, edge, property, map
    const pairs = Object.entries(entry)
      .map(([key, value]) => `${key}: ${parseEntry(value)}`)
      .join(', ');
    return `{${pairs}}`;
  }
  if (Array.isArray(entry)) {
    // Array: path, list
    return entry.map(parseEntry).join(', ');
  }
  // Primitives: bool, int, string, Date, float, etc.
  return String(entry);
}

/**
 * Calculate column widths for table formatting.
 *
 * @param {string[][]} rows - The table rows.
 * @param {string[]} headers - The column headers.
 * @returns {number[]} Array of column widths.
 */
function calculateColumnWidths(rows, headers) {
  const widths = headers.map((h) => h.length);
  for (const row of rows) {
    for (let i = 0; i < row.length; i++) {
      const cellLen = String(row[i] ?? '').length;
      if (cellLen > widths[i]) {
        widths[i] = cellLen;
      }
    }
  }
  return widths;
}

/**
 * Print results as a grid-formatted table (similar to Python's tabulate with tablefmt='grid').
 *
 * @param {string[]} headers - The column headers.
 * @param {string[][]} rows - The table rows.
 */
function printResultsAsTable(headers, rows) {
  const widths = calculateColumnWidths(rows, headers);

  // Build separator line: +------+------+
  const separator = '+' + widths.map((w) => '-'.repeat(w + 2)).join('+') + '+';

  // Build a row line: | val1 | val2 |
  function formatRow(cells) {
    return (
      '| ' +
      cells
        .map((cell, i) => {
          const s = String(cell ?? '');
          return s.padEnd(widths[i]);
        })
        .join(' | ') +
      ' |'
    );
  }

  const lines = [];
  lines.push(separator);
  lines.push(formatRow(headers));
  lines.push(separator);
  for (const row of rows) {
    lines.push(formatRow(row));
  }
  lines.push(separator);

  console.log(lines.join('\n'));
}

/**
 * Parse and format a QueryResult for printing.
 *
 * @param {import('./query-result').QueryResult} queryResult - The query result to format.
 * @param {number} [maxRows=20] - Maximum number of rows to display.
 */
function parseAndFormatResults(queryResult, maxRows = 20) {
  const headers = queryResult.columnNames();
  const rows = [];

  const totalRecords = queryResult.length();
  let displayCount = Math.min(maxRows, totalRecords);

  for (const record of queryResult) {
    const currentRow = [];
    for (const column of record) {
      currentRow.push(parseEntry(column));
    }
    rows.push(currentRow);
    displayCount--;
    if (displayCount <= 0) break;
  }

  if (totalRecords > maxRows && rows.length > 0) {
    rows.push(Array(headers.length).fill('...'));
  }

  printResultsAsTable(headers, rows);
}

module.exports = { parseEntry, printResultsAsTable, parseAndFormatResults };
