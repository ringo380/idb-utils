// Schema viewer — mirrors `inno schema`
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';

export function createSchema(container, fileData) {
  const wasm = getWasm();
  let schema;
  try {
    const raw = wasm.extract_schema(fileData);
    schema = JSON.parse(raw);
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error extracting schema: ${esc(e)}</div>`;
    return;
  }

  if (!schema) {
    container.innerHTML = `
      <div class="p-6">
        <h2 class="text-lg font-bold text-innodb-cyan mb-4">Schema</h2>
        <div class="bg-surface-2 rounded-lg p-6 text-center text-gray-500">
          <p class="mb-2">No schema metadata found.</p>
          <p class="text-xs">Schema extraction requires MySQL 8.0+ tablespaces with SDI metadata.</p>
        </div>
      </div>`;
    return;
  }

  const tableName = schema.table_name || 'unknown';
  const schemaName = schema.schema_name ? `${esc(schema.schema_name)}.` : '';

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Schema</h2>
        <span class="text-sm text-gray-400">${schemaName}${esc(tableName)}</span>
        <span id="schema-export"></span>
      </div>

      ${renderTableInfo(schema)}
      ${renderColumns(schema.columns)}
      ${renderIndexes(schema.indexes)}
      ${schema.foreign_keys && schema.foreign_keys.length > 0 ? renderForeignKeys(schema.foreign_keys) : ''}
      ${renderDdl(schema.ddl)}
    </div>
  `;

  const exportSlot = container.querySelector('#schema-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => schema, 'schema'));
  }

  const copyBtn = container.querySelector('#schema-copy-ddl');
  if (copyBtn && schema.ddl) {
    copyBtn.addEventListener('click', () => {
      navigator.clipboard.writeText(schema.ddl).then(() => {
        copyBtn.textContent = 'Copied!';
        setTimeout(() => { copyBtn.textContent = 'Copy'; }, 2000);
      });
    });
  }
}

function renderTableInfo(schema) {
  const rows = [
    ['Engine', schema.engine],
    ['Row Format', schema.row_format],
    ['Collation', schema.collation],
    ['Charset', schema.charset],
    ['MySQL Version', schema.mysql_version],
    ['Source', schema.source],
  ].filter(([, v]) => v);

  if (rows.length === 0) return '';

  return `
    <div class="bg-surface-2 rounded-lg overflow-hidden">
      <div class="px-4 py-2 border-b border-gray-800">
        <span class="text-sm font-bold text-gray-300">Table Info</span>
      </div>
      <div class="grid grid-cols-2 sm:grid-cols-3 gap-x-6 gap-y-1 px-4 py-3 text-sm">
        ${rows.map(([k, v]) => `<div><span class="text-gray-500">${esc(k)}:</span> <span class="text-gray-300">${esc(String(v))}</span></div>`).join('')}
      </div>
      ${schema.comment ? `<div class="px-4 pb-3 text-xs text-gray-500">Comment: ${esc(schema.comment)}</div>` : ''}
    </div>`;
}

function renderColumns(columns) {
  if (!columns || columns.length === 0) return '';

  return `
    <div class="bg-surface-2 rounded-lg overflow-hidden">
      <div class="px-4 py-2 border-b border-gray-800">
        <span class="text-sm font-bold text-gray-300">Columns</span>
        <span class="text-xs text-gray-500 ml-2">(${columns.length})</span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead>
            <tr class="text-left text-gray-500 text-xs border-b border-gray-800">
              <th class="px-4 py-2">#</th>
              <th class="px-4 py-2">Name</th>
              <th class="px-4 py-2">Type</th>
              <th class="px-4 py-2">Nullable</th>
              <th class="px-4 py-2">Default</th>
              <th class="px-4 py-2">Extra</th>
            </tr>
          </thead>
          <tbody>
            ${columns.map((col, i) => renderColumnRow(col, i)).join('')}
          </tbody>
        </table>
      </div>
    </div>`;
}

function renderColumnRow(col, idx) {
  const extras = [];
  if (col.is_auto_increment) extras.push('AUTO_INCREMENT');
  if (col.is_invisible) extras.push('INVISIBLE');
  if (col.generation_expression) {
    extras.push(`${col.is_virtual ? 'VIRTUAL' : 'STORED'}: ${col.generation_expression}`);
  }

  return `
    <tr class="border-b border-gray-800/50 hover:bg-surface-3/30">
      <td class="px-4 py-1.5 text-gray-600">${idx + 1}</td>
      <td class="px-4 py-1.5 text-innodb-cyan font-mono text-xs">${esc(col.name)}</td>
      <td class="px-4 py-1.5 text-gray-300 font-mono text-xs">${esc(col.column_type)}</td>
      <td class="px-4 py-1.5 ${col.is_nullable ? 'text-gray-400' : 'text-gray-600'}">${col.is_nullable ? 'YES' : 'NO'}</td>
      <td class="px-4 py-1.5 text-gray-400 font-mono text-xs">${col.default_value != null ? esc(col.default_value) : '<span class="text-gray-600">-</span>'}</td>
      <td class="px-4 py-1.5 text-gray-500 text-xs">${extras.length > 0 ? esc(extras.join(', ')) : ''}</td>
    </tr>`;
}

function renderIndexes(indexes) {
  if (!indexes || indexes.length === 0) return '';

  return `
    <div class="bg-surface-2 rounded-lg overflow-hidden">
      <div class="px-4 py-2 border-b border-gray-800">
        <span class="text-sm font-bold text-gray-300">Indexes</span>
        <span class="text-xs text-gray-500 ml-2">(${indexes.length})</span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead>
            <tr class="text-left text-gray-500 text-xs border-b border-gray-800">
              <th class="px-4 py-2">Name</th>
              <th class="px-4 py-2">Type</th>
              <th class="px-4 py-2">Columns</th>
            </tr>
          </thead>
          <tbody>
            ${indexes.map((idx) => renderIndexRow(idx)).join('')}
          </tbody>
        </table>
      </div>
    </div>`;
}

function renderIndexRow(idx) {
  const cols = idx.columns
    .map((c) => {
      let s = c.name;
      if (c.prefix_length) s += `(${c.prefix_length})`;
      if (c.order === 'DESC') s += ' DESC';
      return s;
    })
    .join(', ');

  return `
    <tr class="border-b border-gray-800/50 hover:bg-surface-3/30">
      <td class="px-4 py-1.5 text-innodb-cyan font-mono text-xs">${esc(idx.name)}</td>
      <td class="px-4 py-1.5 text-gray-300 text-xs">${esc(idx.index_type)}</td>
      <td class="px-4 py-1.5 text-gray-400 font-mono text-xs">${esc(cols)}</td>
    </tr>`;
}

function renderForeignKeys(fks) {
  return `
    <div class="bg-surface-2 rounded-lg overflow-hidden">
      <div class="px-4 py-2 border-b border-gray-800">
        <span class="text-sm font-bold text-gray-300">Foreign Keys</span>
        <span class="text-xs text-gray-500 ml-2">(${fks.length})</span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead>
            <tr class="text-left text-gray-500 text-xs border-b border-gray-800">
              <th class="px-4 py-2">Name</th>
              <th class="px-4 py-2">Columns</th>
              <th class="px-4 py-2">References</th>
              <th class="px-4 py-2">On Delete</th>
              <th class="px-4 py-2">On Update</th>
            </tr>
          </thead>
          <tbody>
            ${fks.map((fk) => `
              <tr class="border-b border-gray-800/50 hover:bg-surface-3/30">
                <td class="px-4 py-1.5 text-innodb-cyan font-mono text-xs">${esc(fk.name)}</td>
                <td class="px-4 py-1.5 text-gray-300 font-mono text-xs">${esc((fk.columns || []).join(', '))}</td>
                <td class="px-4 py-1.5 text-gray-400 font-mono text-xs">${esc(fk.referenced_table || '')}(${esc((fk.referenced_columns || []).join(', '))})</td>
                <td class="px-4 py-1.5 text-gray-500 text-xs">${esc(fk.on_delete || '')}</td>
                <td class="px-4 py-1.5 text-gray-500 text-xs">${esc(fk.on_update || '')}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </div>`;
}

function renderDdl(ddl) {
  if (!ddl) return '';

  return `
    <div class="bg-surface-2 rounded-lg overflow-hidden">
      <div class="px-4 py-2 border-b border-gray-800 flex items-center justify-between">
        <span class="text-sm font-bold text-gray-300">DDL</span>
        <button id="schema-copy-ddl" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-400 rounded text-xs">Copy</button>
      </div>
      <pre class="px-4 py-3 text-xs text-gray-300 overflow-x-auto max-h-96 font-mono">${esc(ddl)}</pre>
    </div>`;
}
