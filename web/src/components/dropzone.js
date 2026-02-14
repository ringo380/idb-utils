// Drag-and-drop file input for .ibd files

export function createDropzone(onFile, onDiffFiles) {
  const el = document.createElement('div');
  el.id = 'dropzone';
  el.className =
    'flex-1 flex flex-col items-center justify-center border-2 border-dashed border-gray-700 rounded-xl m-8 p-12 transition-colors cursor-pointer';
  el.innerHTML = `
    <svg class="w-16 h-16 text-gray-600 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"
        d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
    </svg>
    <p class="text-lg text-gray-400 mb-2">Drop an .ibd file here to analyze</p>
    <p class="text-sm text-gray-600 mb-4">or drop two files to diff them</p>
    <button id="file-btn"
      class="px-4 py-2 bg-gray-800 hover:bg-gray-700 text-gray-300 rounded-lg text-sm transition-colors">
      Choose File
    </button>
    <input type="file" id="file-input" accept=".ibd,.ibu,.ib_logfile0,.ib_logfile1" multiple class="hidden" />
  `;

  const input = el.querySelector('#file-input');
  const btn = el.querySelector('#file-btn');

  btn.addEventListener('click', () => input.click());
  input.addEventListener('change', () => handleFiles(input.files));

  el.addEventListener('dragover', (e) => {
    e.preventDefault();
    el.classList.add('dropzone-active');
  });
  el.addEventListener('dragleave', () => {
    el.classList.remove('dropzone-active');
  });
  el.addEventListener('drop', (e) => {
    e.preventDefault();
    el.classList.remove('dropzone-active');
    handleFiles(e.dataTransfer.files);
  });

  function handleFiles(files) {
    if (!files || files.length === 0) return;
    if (files.length >= 2) {
      Promise.all([readFile(files[0]), readFile(files[1])]).then(([a, b]) =>
        onDiffFiles(files[0].name, a, files[1].name, b)
      );
    } else {
      readFile(files[0]).then((data) => onFile(files[0].name, data));
    }
  }

  return el;
}

function readFile(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(new Uint8Array(reader.result));
    reader.onerror = reject;
    reader.readAsArrayBuffer(file);
  });
}
