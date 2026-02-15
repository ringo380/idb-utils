// Tab navigation bar

const TAB_DEFS = [
  { id: 'overview', label: 'Overview', key: '1' },
  { id: 'pages', label: 'Pages', key: '2' },
  { id: 'checksums', label: 'Checksums', key: '3' },
  { id: 'sdi', label: 'SDI', key: '4' },
  { id: 'hex', label: 'Hex Dump', key: '5' },
  { id: 'recovery', label: 'Recovery', key: '6' },
];

const DIFF_TAB = { id: 'diff', label: 'Diff', key: '7' };

export function createTabs(onSwitch, { showDiff = false } = {}) {
  const tabs = showDiff ? [...TAB_DEFS, DIFF_TAB] : [...TAB_DEFS];
  const el = document.createElement('nav');
  el.className = 'flex gap-1 px-4 pt-3 pb-0 bg-gray-950 border-b border-gray-800 overflow-x-auto';

  tabs.forEach((tab, i) => {
    const btn = document.createElement('button');
    btn.dataset.tab = tab.id;
    btn.className =
      'px-3 py-2 text-sm text-gray-500 hover:text-gray-300 border-b-2 border-transparent transition-colors whitespace-nowrap';
    btn.textContent = `${tab.key} ${tab.label}`;
    btn.addEventListener('click', () => onSwitch(i));
    el.appendChild(btn);
  });

  return el;
}

export function setActiveTab(nav, index) {
  const buttons = nav.querySelectorAll('button');
  buttons.forEach((btn, i) => {
    if (i === index) {
      btn.classList.add('tab-active');
      btn.classList.remove('text-gray-500');
    } else {
      btn.classList.remove('tab-active');
      btn.classList.add('text-gray-500');
    }
  });
}

export function getTabId(index) {
  const all = [...TAB_DEFS, DIFF_TAB];
  return all[index]?.id;
}

export function getTabCount(showDiff) {
  return showDiff ? TAB_DEFS.length + 1 : TAB_DEFS.length;
}
