// --- State Management ---
const state = {
    instances: [], // { id, name, url, token, enabled }
    logs: [],      // Aggregated live logs
    activeConnections: {}, // id -> WebSocket
    logCount: 0,
    logsThisSecond: 0,
    currentTab: 'live',
    csvData: { headers: [], rows: [] },
    autoScroll: true
};

// --- Initializer ---
document.addEventListener('DOMContentLoaded', () => {
    // Load instances from localStorage
    loadInstances();
    
    // Setup Lucide Icons
    lucide.createIcons();

    // Tab Navigation
    const navItems = document.querySelectorAll('.nav-item');
    navItems.forEach(item => {
        item.addEventListener('click', () => {
            switchTab(item.getAttribute('data-tab'));
        });
    });

    // Form submission
    const addForm = document.getElementById('add-instance-form');
    addForm.addEventListener('submit', handleAddInstance);

    // Live controls listeners
    document.getElementById('btn-clear-console').addEventListener('click', clearConsole);
    document.getElementById('btn-toggle-scroll').addEventListener('click', toggleAutoScroll);
    document.getElementById('live-search').addEventListener('input', applyLiveFilters);
    document.getElementById('filter-level').addEventListener('change', applyLiveFilters);
    document.getElementById('filter-category').addEventListener('change', applyLiveFilters);
    document.getElementById('filter-instance').addEventListener('change', applyLiveFilters);

    // Historical loader listeners
    document.getElementById('btn-load-csv').addEventListener('click', loadCSVHistory);
    document.getElementById('history-search').addEventListener('input', applyHistorySearch);

    // Modal Close
    document.getElementById('close-modal-btn').addEventListener('click', closeModal);
    document.getElementById('log-detail-modal').addEventListener('click', (e) => {
        if (e.target.id === 'log-detail-modal') closeModal();
    });

    // Calculate logs/sec rate
    setInterval(updateLogRate, 1000);

    // Render registered instances list
    renderInstancesList();
    
    // Connect to enabled instances on startup
    connectAllEnabledInstances();
});

// --- Tab Swapper ---
function switchTab(tabName) {
    state.currentTab = tabName;

    // Update Nav UI
    document.querySelectorAll('.nav-item').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-tab') === tabName);
    });

    // Update Tab Content UI
    document.querySelectorAll('.tab-content').forEach(section => {
        section.classList.toggle('active', section.id === `tab-${tabName}`);
    });

    // Update Header Title/Desc
    const titleEl = document.getElementById('current-tab-title');
    const descEl = document.getElementById('current-tab-desc');
    const statsEl = document.getElementById('live-stats');

    if (tabName === 'live') {
        titleEl.textContent = 'Console em Tempo Real';
        descEl.textContent = 'Transmissão agregada de logs das instâncias conectadas.';
        statsEl.style.display = 'flex';
    } else if (tabName === 'history') {
        titleEl.textContent = 'Explorador Histórico (CSV)';
        descEl.textContent = 'Baixe e analise relatórios de log no formato CSV exportados pelo Power BI.';
        statsEl.style.display = 'none';
        populateInstanceSelects();
    } else if (tabName === 'instances') {
        titleEl.textContent = 'Gerenciar Instâncias';
        descEl.textContent = 'Adicione, exclua e ative o monitoramento de instâncias remotas.';
        statsEl.style.display = 'none';
    }
}

// --- LocalStorage Loader/Saver ---
function loadInstances() {
    const saved = localStorage.getItem('simcc_instances');
    if (saved) {
        try {
            state.instances = JSON.parse(saved);
        } catch (e) {
            state.instances = [];
        }
    } else {
        // Add default connection to the current instance
        const defaultOrigin = (window && window.location && window.location.origin && window.location.origin !== 'null') 
            ? window.location.origin 
            : 'http://localhost:8000';
        state.instances = [
            {
                id: 'inst-default-local',
                name: 'Esta Instância',
                url: defaultOrigin,
                token: '',
                enabled: true
            }
        ];
        saveInstances();
    }
}

function saveInstances() {
    localStorage.setItem('simcc_instances', JSON.stringify(state.instances));
}

// --- Add Instance ---
function handleAddInstance(e) {
    e.preventDefault();
    
    const name = document.getElementById('inst-name').value.trim();
    let url = document.getElementById('inst-url').value.trim();
    const token = document.getElementById('inst-token').value.trim();

    // Clean trailing slash
    if (url.endsWith('/')) {
        url = url.slice(0, -1);
    }

    const newInst = {
        id: 'inst-' + Date.now(),
        name,
        url,
        token,
        enabled: true
    };

    state.instances.push(newInst);
    saveInstances();
    
    // Reset Form
    e.target.reset();

    // Re-render
    renderInstancesList();
    populateInstanceSelects();

    // Automatically connect
    connectInstance(newInst);
}

// --- Delete Instance ---
function deleteInstance(id) {
    // Disconnect if active
    disconnectInstance(id);

    state.instances = state.instances.filter(inst => inst.id !== id);
    saveInstances();
    
    renderInstancesList();
    populateInstanceSelects();
}

// --- Toggle Instance Connection ---
function toggleInstance(id, enabled) {
    const idx = state.instances.findIndex(inst => inst.id === id);
    if (idx !== -1) {
        state.instances[idx].enabled = enabled;
        saveInstances();
        
        if (enabled) {
            connectInstance(state.instances[idx]);
        } else {
            disconnectInstance(id);
        }
    }
    updateGlobalStatusBadge();
}

// --- WebSocket Connections ---
function connectAllEnabledInstances() {
    state.instances.forEach(inst => {
        if (inst.enabled) {
            connectInstance(inst);
        }
    });
}

function connectInstance(inst) {
    // Avoid double connections
    if (state.activeConnections[inst.id]) return;

    // Infer WebSocket URL
    let wsUrl = '';
    if (inst.url.startsWith('https://')) {
        wsUrl = inst.url.replace('https://', 'wss://') + '/logs/stream';
    } else if (inst.url.startsWith('http://')) {
        wsUrl = inst.url.replace('http://', 'ws://') + '/logs/stream';
    } else {
        // Fallback
        wsUrl = 'ws://' + inst.url + '/logs/stream';
    }

    // Attach authentication token if specified
    if (inst.token) {
        wsUrl += `?token=${encodeURIComponent(inst.token)}`;
    }

    updateInstanceUIStatus(inst.id, 'connecting', 'Conectando...');

    try {
        const ws = new WebSocket(wsUrl);
        state.activeConnections[inst.id] = ws;

        ws.onopen = () => {
            updateInstanceUIStatus(inst.id, 'connected', 'Conectado');
            updateGlobalStatusBadge();
        };

        ws.onmessage = (event) => {
            try {
                const logData = JSON.parse(event.data);
                appendLog(inst.name, logData);
            } catch (e) {
                // Log non-JSON raw strings if any
                appendLog(inst.name, {
                    timestamp: new Date().toISOString(),
                    level: 'debug',
                    category: 'system',
                    event: 'raw.text',
                    message: event.data,
                    data: {}
                });
            }
        };

        ws.onclose = (event) => {
            delete state.activeConnections[inst.id];
            updateInstanceUIStatus(inst.id, 'disconnected', 'Desconectado');
            updateGlobalStatusBadge();
            
            // Auto-reconnect if still enabled (after 5 seconds)
            setTimeout(() => {
                const currentInst = state.instances.find(i => i.id === inst.id);
                if (currentInst && currentInst.enabled) {
                    connectInstance(currentInst);
                }
            }, 5000);
        };

        ws.onerror = (err) => {
            updateInstanceUIStatus(inst.id, 'error', 'Falha na Conexão');
        };
    } catch (e) {
        updateInstanceUIStatus(inst.id, 'error', 'Erro');
    }
}

function disconnectInstance(id) {
    if (state.activeConnections[id]) {
        state.activeConnections[id].close();
        delete state.activeConnections[id];
    }
    updateInstanceUIStatus(id, 'disconnected', 'Desconectado');
    updateGlobalStatusBadge();
}

// --- Live Logs Processor ---
function appendLog(instanceName, logData) {
    state.logCount++;
    state.logsThisSecond++;
    
    // Add instance metadata
    logData.instanceName = instanceName;
    logData.uniqueId = 'log-' + state.logCount + '-' + Math.random().toString(36).substr(2, 5);

    // Keep log array max cap to 1000 items to avoid lagging browser memory
    state.logs.push(logData);
    if (state.logs.length > 1000) {
        state.logs.shift();
    }

    // Refresh stat counter elements
    document.getElementById('stat-total').textContent = state.logCount;

    // Fast-append if it matches filters
    if (matchesFilters(logData)) {
        appendLogRowToFeed(logData);
    }
}

function updateLogRate() {
    document.getElementById('stat-rate').textContent = state.logsThisSecond.toFixed(1);
    state.logsThisSecond = 0;
}

// --- Filter Checks ---
function matchesFilters(log) {
    const searchVal = document.getElementById('live-search').value.toLowerCase();
    const filterLevel = document.getElementById('filter-level').value;
    const filterCat = document.getElementById('filter-category').value;
    const filterInst = document.getElementById('filter-instance').value;

    if (filterLevel && log.level !== filterLevel) return false;
    if (filterCat && log.category !== filterCat) return false;
    if (filterInst && log.instanceName !== filterInst) return false;

    if (searchVal) {
        const msg = (log.message || '').toLowerCase();
        const ev = (log.event || '').toLowerCase();
        const cat = (log.category || '').toLowerCase();
        const dataStr = JSON.stringify(log.data || {}).toLowerCase();
        
        return msg.includes(searchVal) || ev.includes(searchVal) || cat.includes(searchVal) || dataStr.includes(searchVal);
    }

    return true;
}

// --- Render Live Log Stream Feed ---
function appendLogRowToFeed(log) {
    const feed = document.getElementById('console-stream-feed');
    
    // Remove empty state if present
    const emptyState = feed.querySelector('.empty-state');
    if (emptyState) {
        feed.innerHTML = '';
    }

    const row = document.createElement('div');
    row.className = `log-row`;
    row.setAttribute('data-id', log.uniqueId);

    // Formatar data abreviada
    let timeStr = '';
    try {
        const d = new Date(log.timestamp);
        timeStr = d.toLocaleTimeString() + '.' + String(d.getMilliseconds()).padStart(3, '0');
    } catch(e) {
        timeStr = log.timestamp;
    }

    row.innerHTML = `
        <div class="col-time">${timeStr}</div>
        <div class="col-instance" title="${log.instanceName}">${log.instanceName}</div>
        <div class="col-level"><span class="badge-level ${log.level}">${log.level}</span></div>
        <div class="col-category"><span class="badge-category ${log.category}">${log.category}</span></div>
        <div class="col-event" title="${log.event}">${log.event}</div>
        <div class="col-message" title="${escapeHTML(log.message)}">${escapeHTML(log.message)}</div>
    `;

    row.addEventListener('click', () => showLogDetail(log));

    feed.appendChild(row);

    // Scroll to bottom if autoScroll is enabled
    if (state.autoScroll) {
        feed.scrollTop = feed.scrollHeight;
    }

    // Keep console DOM clean (keep only last 100 rows in viewport)
    while (feed.children.length > 250) {
        feed.removeChild(feed.firstChild);
    }
}

function applyLiveFilters() {
    const feed = document.getElementById('console-stream-feed');
    feed.innerHTML = '';
    
    const filtered = state.logs.filter(matchesFilters);
    
    if (filtered.length === 0) {
        feed.innerHTML = `
            <div class="empty-state">
                <i data-lucide="filter"></i>
                <h3>Nenhum resultado corresponde aos filtros</h3>
                <p>Ajuste os filtros ou insira uma nova busca.</p>
            </div>
        `;
        lucide.createIcons();
    } else {
        filtered.forEach(log => appendLogRowToFeed(log));
    }
}

// --- Console Control Actions ---
function clearConsole() {
    state.logs = [];
    applyLiveFilters();
}

function toggleAutoScroll() {
    state.autoScroll = !state.autoScroll;
    const btn = document.getElementById('btn-toggle-scroll');
    btn.classList.toggle('active', state.autoScroll);
}

// --- Render Instances UI ---
function renderInstancesList() {
    const feed = document.getElementById('instances-list-feed');
    feed.innerHTML = '';

    if (state.instances.length === 0) {
        feed.innerHTML = `
            <div class="empty-state">
                <i data-lucide="server"></i>
                <h3>Nenhuma instância cadastrada</h3>
                <p>Insira os dados de uma nova instância ao lado para começar.</p>
            </div>
        `;
        lucide.createIcons();
        return;
    }

    state.instances.forEach(inst => {
        const card = document.createElement('div');
        card.className = 'instance-card';
        card.innerHTML = `
            <div class="instance-info">
                <div class="instance-info-header">
                    <h4>${inst.name}</h4>
                    <span class="indicator red" id="indicator-${inst.id}"></span>
                    <span class="badge-level debug" id="status-text-${inst.id}">Desconectado</span>
                </div>
                <p>${inst.url}</p>
            </div>
            <div class="instance-actions">
                <label class="switch">
                    <input type="checkbox" id="switch-${inst.id}" ${inst.enabled ? 'checked' : ''}>
                    <span class="slider"></span>
                </label>
                <button class="btn-icon" title="Excluir Instância" onclick="deleteInstance('${inst.id}')">
                    <i data-lucide="trash"></i>
                </button>
            </div>
        `;

        // Toggle connection switch listener
        card.querySelector(`#switch-${inst.id}`).addEventListener('change', (e) => {
            toggleInstance(inst.id, e.target.checked);
        });

        feed.appendChild(card);
    });

    lucide.createIcons();
    updateGlobalStatusBadge();
    
    // Sync status indicators
    state.instances.forEach(inst => {
        const isConnected = !!state.activeConnections[inst.id];
        if (isConnected) {
            updateInstanceUIStatus(inst.id, 'connected', 'Conectado');
        } else if (inst.enabled) {
            updateInstanceUIStatus(inst.id, 'connecting', 'Conectando...');
        }
    });
}

function updateInstanceUIStatus(id, status, text) {
    const ind = document.getElementById(`indicator-${id}`);
    const txt = document.getElementById(`status-text-${id}`);
    if (!ind || !txt) return;

    ind.className = 'indicator';
    txt.className = 'badge-level';
    
    if (status === 'connected') {
        ind.classList.add('green');
        txt.classList.add('debug');
        txt.textContent = 'Conectado';
    } else if (status === 'connecting') {
        ind.classList.add('yellow');
        txt.classList.add('debug');
        txt.textContent = 'Conectando';
    } else if (status === 'error') {
        ind.classList.add('red');
        txt.classList.add('error');
        txt.textContent = 'Erro';
    } else {
        ind.classList.add('red');
        txt.classList.add('debug');
        txt.textContent = 'Desconectado';
    }
}

function updateGlobalStatusBadge() {
    const activeCount = Object.keys(state.activeConnections).length;
    const totalEnabled = state.instances.filter(i => i.enabled).length;

    const ind = document.getElementById('global-status-indicator');
    const txt = document.getElementById('global-status-text');
    
    ind.className = 'indicator';
    
    if (activeCount > 0) {
        ind.classList.add('green');
        txt.textContent = `${activeCount} de ${totalEnabled} instâncias conectadas`;
    } else if (totalEnabled > 0) {
        ind.classList.add('yellow');
        txt.textContent = `Conectando a ${totalEnabled} instâncias...`;
    } else {
        ind.classList.add('red');
        txt.textContent = 'Sem conexões ativas';
    }

    // Refresh filters options
    const select = document.getElementById('filter-instance');
    const currentVal = select.value;
    select.innerHTML = '<option value="">Todas as Instâncias</option>';
    state.instances.forEach(i => {
        select.innerHTML += `<option value="${i.name}">${i.name}</option>`;
    });
    select.value = currentVal;
    
    // Update live stat stats connection label
    const statConnections = document.getElementById('stat-connections');
    if (statConnections) {
        statConnections.textContent = `${activeCount}/${state.instances.length}`;
    }
}

function populateInstanceSelects() {
    const select = document.getElementById('history-instance-select');
    const currentVal = select.value;
    select.innerHTML = '<option value="" disabled selected>Selecione uma instância...</option>';
    state.instances.forEach(i => {
        select.innerHTML += `<option value="${i.id}">${i.name}</option>`;
    });
    if (state.instances.some(i => i.id === currentVal)) {
        select.value = currentVal;
    }
}

// --- Historical CSV Loader & Parser ---
async function loadCSVHistory() {
    const instId = document.getElementById('history-instance-select').value;
    const csvFile = document.getElementById('history-csv-select').value;

    if (!instId) {
        alert('Por favor, selecione uma instância primeiro.');
        return;
    }

    const inst = state.instances.find(i => i.id === instId);
    if (!inst) return;

    const loadBtn = document.getElementById('btn-load-csv');
    loadBtn.disabled = true;
    loadBtn.innerHTML = '<i data-lucide="loader" class="animate-spin"></i> <span>Baixando...</span>';
    lucide.createIcons();

    // Mount Fetch request
    const csvUrl = `${inst.url}/${csvFile}`;
    
    try {
        const response = await fetch(csvUrl);
        if (!response.ok) {
            throw new Error(`Falha ao buscar CSV (HTTP ${response.status})`);
        }
        
        const csvText = await response.text();
        const parsed = parseCSV(csvText);
        state.csvData = parsed;

        renderCSVTable();
    } catch (e) {
        alert(`Erro ao carregar CSV: ${e.message}`);
        // Render Empty state table
        document.getElementById('csv-table-body').innerHTML = `
            <tr>
                <td colspan="100" class="table-empty">
                    <i data-lucide="alert-triangle" class="color-error"></i>
                    <p>Falha ao conectar com o endpoint da instância ou arquivo indisponível: ${e.message}</p>
                </td>
            </tr>
        `;
        document.getElementById('history-table-meta').style.display = 'none';
        lucide.createIcons();
    } finally {
        loadBtn.disabled = false;
        loadBtn.innerHTML = '<i data-lucide="download-cloud"></i> <span>Carregar Dados</span>';
        lucide.createIcons();
    }
}

// Custom Client-Side CSV Parser
function parseCSV(text) {
    if (!text) return { headers: [], rows: [] };
    
    // Auto-detect delimiter (count occurrences in the first line)
    const firstLine = text.split('\n')[0] || '';
    const semicolons = (firstLine.match(/;/g) || []).length;
    const commas = (firstLine.match(/,/g) || []).length;
    const delimiter = semicolons > commas ? ';' : ',';

    const lines = [];
    let row = [];
    let inQuotes = false;
    let entry = "";

    // Simple state machine for parsing fields with potential quotes and newlines
    for (let i = 0; i < text.length; i++) {
        const char = text[i];
        const nextChar = text[i + 1];

        if (inQuotes) {
            if (char === '"') {
                if (nextChar === '"') {
                    entry += '"';
                    i++; // Skip double quote
                } else {
                    inQuotes = false;
                }
            } else {
                entry += char;
            }
        } else {
            if (char === '"') {
                inQuotes = true;
            } else if (char === delimiter) {
                row.push(entry);
                entry = "";
            } else if (char === '\n' || char === '\r') {
                if (char === '\r' && nextChar === '\n') {
                    i++;
                }
                row.push(entry);
                if (row.length > 1 || row[0] !== "") {
                    lines.push(row);
                }
                row = [];
                entry = "";
            } else {
                entry += char;
            }
        }
    }
    
    // Add last entry if any
    if (entry || row.length > 1 || row[0] !== "") {
        row.push(entry);
        lines.push(row);
    }

    if (lines.length === 0) return { headers: [], rows: [] };
    
    // Normalize header names
    const headers = lines[0].map((h, index) => {
        // If header is empty (index column), call it "index"
        return h.trim() === "" ? 'index' : h.trim();
    });

    const result = [];
    for (let i = 1; i < lines.length; i++) {
        const line = lines[i];
        if (line.length < headers.length) continue;
        const obj = {};
        headers.forEach((header, index) => {
            obj[header] = line[index] ? line[index].trim() : '';
        });
        result.push(obj);
    }
    return { headers, rows: result };
}

function renderCSVTable() {
    const headerRow = document.getElementById('csv-table-headers');
    const tableBody = document.getElementById('csv-table-body');
    const metaPanel = document.getElementById('history-table-meta');
    
    headerRow.innerHTML = '';
    tableBody.innerHTML = '';

    if (state.csvData.rows.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="100" class="table-empty">
                    <i data-lucide="info"></i>
                    <p>O arquivo CSV foi baixado, mas não contém nenhuma linha de dados.</p>
                </td>
            </tr>
        `;
        metaPanel.style.display = 'none';
        lucide.createIcons();
        return;
    }

    // Build Headers
    state.csvData.headers.forEach(h => {
        const th = document.createElement('th');
        th.textContent = h === 'index' ? '' : h; // Show index column header as blank
        headerRow.appendChild(th);
    });

    // Build Rows
    const filterText = document.getElementById('history-search').value.toLowerCase();
    let rowCount = 0;

    state.csvData.rows.forEach(rowObj => {
        // Filter rows based on search
        if (filterText) {
            const match = Object.values(rowObj).some(val => String(val).toLowerCase().includes(filterText));
            if (!match) return;
        }

        rowCount++;
        const tr = document.createElement('tr');
        state.csvData.headers.forEach(header => {
            const td = document.createElement('td');
            const val = rowObj[header] || '';
            td.textContent = val;
            td.title = val;
            tr.appendChild(td);
        });
        tableBody.appendChild(tr);
    });

    // Update loaded info text
    document.getElementById('csv-loaded-info').textContent = `Total carregado: ${rowCount} de ${state.csvData.rows.length} registros.`;
    metaPanel.style.display = 'flex';
}

function applyHistorySearch() {
    renderCSVTable();
}

// --- Detail Log Modal ---
function showLogDetail(log) {
    const modal = document.getElementById('log-detail-modal');
    
    // Fill Meta
    const lvlEl = document.getElementById('modal-log-level');
    lvlEl.className = `badge-level ${log.level}`;
    lvlEl.textContent = log.level;

    document.getElementById('modal-log-instance').textContent = log.instanceName;
    document.getElementById('modal-log-time').textContent = log.timestamp;
    document.getElementById('modal-log-category').textContent = log.category;
    document.getElementById('modal-log-message').textContent = log.message;

    // Fill JSON payload (data)
    document.getElementById('modal-log-payload').textContent = JSON.stringify(log.data || {}, null, 2);
    
    // Fill Raw log record
    const { instanceName, uniqueId, ...rawLog } = log;
    document.getElementById('modal-log-raw').textContent = JSON.stringify(rawLog, null, 2);

    modal.classList.add('active');
}

function closeModal() {
    document.getElementById('log-detail-modal').classList.remove('active');
}

// --- Helper Functions ---
function escapeHTML(str) {
    if (!str) return '';
    return str.replace(/[&<>'"]/g, 
        tag => ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            "'": '&#39;',
            '"': '&quot;'
        }[tag] || tag)
    );
}
