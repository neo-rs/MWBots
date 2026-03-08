/**
 * Channel Bridge - UNIFIED Discord API Source
 * ============================================
 * 
 * ALL Discord API calls go through this module.
 * Uses DISCUM_BOT (user token) for guilds & channels (all servers)
 * Uses NEO token for RS Server specific data (emojis, messages)
 * Caches results to avoid duplicate API calls.
 * 
 * Other modules should import this and use:
 *   - ChannelBridge.getGuilds()      → DISCUM_BOT token
 *   - ChannelBridge.getChannels(guildId) → DISCUM_BOT token
 *   - ChannelBridge.getEmojis(guildId)   → NEO token (RS Server only)
 *   - ChannelBridge.getMessages(channelId, options) → NEO token
 */

class ChannelBridgeAPI {
    constructor() {
        // API base from config.js (load before this script). Empty = same origin.
        this.API_BASE = (typeof window !== 'undefined' && window.CHANNELMAP_API_BASE !== undefined) ? String(window.CHANNELMAP_API_BASE) : '';
        
        // Cache with TTL
        this.cache = {
            guilds: { data: null, timestamp: 0 },
            channels: {},  // keyed by guild_id
            emojis: {},    // keyed by guild_id
            messages: {},  // keyed by channel_id
            envStatus: { data: null, timestamp: 0 }
        };
        
        // Cache TTL in milliseconds
        this.CACHE_TTL = {
            guilds: 5 * 60 * 1000,    // 5 minutes
            channels: 5 * 60 * 1000,  // 5 minutes
            emojis: 60 * 60 * 1000,   // 1 hour
            messages: 30 * 1000,      // 30 seconds
            envStatus: 60 * 1000      // 1 minute
        };
        
        // Pending requests - prevents duplicate simultaneous API calls
        this._pendingRequests = {
            guilds: null,
            channels: {},  // keyed by guild_id
            emojis: {},
            messages: {},
            envStatus: null,
            botStatus: null
        };
        
        // Channel mapping state
        this.selectedSourceChannels = new Set();
        this.channelMap = {};
        this.sourceChannels = [];
        this.destChannels = [];
        this.sourceServerId = null;
        this.destServerId = null;
        
        console.log('[ChannelBridge] Unified API initialized with request deduplication');
    }
    
    // ========================================================================
    // CACHE UTILITIES
    // ========================================================================
    
    _isCacheValid(cacheEntry, ttl) {
        if (!cacheEntry || !cacheEntry.data) return false;
        return (Date.now() - cacheEntry.timestamp) < ttl;
    }
    
    _setCache(type, key, data) {
        if (key) {
            if (!this.cache[type]) this.cache[type] = {};
            this.cache[type][key] = { data, timestamp: Date.now() };
        } else {
            this.cache[type] = { data, timestamp: Date.now() };
        }
    }
    
    _getCache(type, key = null) {
        const ttl = this.CACHE_TTL[type] || 60000;
        if (key) {
            const entry = this.cache[type]?.[key];
            return this._isCacheValid(entry, ttl) ? entry.data : null;
        } else {
            return this._isCacheValid(this.cache[type], ttl) ? this.cache[type].data : null;
        }
    }
    
    clearCache(type = null) {
        if (type) {
            this.cache[type] = type === 'guilds' || type === 'envStatus' 
                ? { data: null, timestamp: 0 } 
                : {};
        } else {
            this.cache = {
                guilds: { data: null, timestamp: 0 },
                channels: {},
                emojis: {},
                messages: {},
                envStatus: { data: null, timestamp: 0 }
            };
        }
        console.log('[ChannelBridge] Cache cleared:', type || 'all');
    }
    
    // ========================================================================
    // UNIFIED DISCORD API METHODS
    // - Guilds/Channels: DISCUM_BOT (user token - all servers)
    // - Emojis/Messages: NEO token (RS Server only)
    // ========================================================================
    
    /**
     * Get all guilds accessible via DISCUM_BOT user token
     * @returns {Promise<Array>} List of guilds
     */
    async getGuilds() {
        // Check cache first
        const cached = this._getCache('guilds');
        if (cached) {
            console.log('[ChannelBridge] Using cached guilds');
            return cached;
        }
        
        // Check if request already in flight (request deduplication)
        if (this._pendingRequests.guilds) {
            console.log('[ChannelBridge] Waiting for pending guilds request...');
            return this._pendingRequests.guilds;
        }
        
        // Create new request and store promise
        this._pendingRequests.guilds = (async () => {
            try {
                const response = await fetch(`${this.API_BASE}/api/discord/guilds`);
                if (!response.ok) {
                    throw new Error(`API error: ${response.status}`);
                }
                
                const data = await response.json();
                const guilds = data.guilds || [];
                
                this._setCache('guilds', null, guilds);
                console.log(`[ChannelBridge] Fetched ${guilds.length} guilds`);
                
                return guilds;
            } catch (error) {
                console.error('[ChannelBridge] Failed to fetch guilds:', error);
                return this._getFallbackGuilds();
            } finally {
                // Clear pending request
                this._pendingRequests.guilds = null;
            }
        })();
        
        return this._pendingRequests.guilds;
    }
    
    /**
     * Fallback guild data when Discord API fails
     */
    _getFallbackGuilds() {
        console.log('[ChannelBridge] Using fallback guild data');
        const assets = (typeof window !== 'undefined' && window.CHANNELMAP_ASSETS) ? String(window.CHANNELMAP_ASSETS) : './assets';
        return [
            {
                id: 'mirrorworld',
                name: 'Mirror World',
                icon: null,
                icon_url: assets + '/images/mirrorworld-default.jpg',
                fallback: true
            },
            {
                id: 'resellingsecrets',
                name: 'Reselling Secrets',
                icon: null,
                icon_url: assets + '/images/rs-default.png',
                fallback: true
            }
        ];
    }
    
    /**
     * Get channels for a guild via DISCUM_BOT user token
     * @param {string} guildId - Discord guild ID
     * @returns {Promise<Array>} List of channels
     */
    async getChannels(guildId) {
        if (!guildId) return [];
        
        // Check cache first
        const cached = this._getCache('channels', guildId);
        if (cached) {
            console.log(`[ChannelBridge] Using cached channels for ${guildId}`);
            return cached;
        }
        
        // Check if request already in flight (request deduplication)
        if (this._pendingRequests.channels[guildId]) {
            console.log(`[ChannelBridge] Waiting for pending channels request for ${guildId}...`);
            return this._pendingRequests.channels[guildId];
        }
        
        // Create new request and store promise
        this._pendingRequests.channels[guildId] = (async () => {
            try {
                const response = await fetch(`${this.API_BASE}/api/discord/channels/${guildId}`);
                if (!response.ok) {
                    throw new Error(`API error: ${response.status}`);
                }
                
                const channels = await response.json();
                
                this._setCache('channels', guildId, channels);
                console.log(`[ChannelBridge] Fetched ${channels.length} channels for ${guildId}`);
                
                return channels;
            } catch (error) {
                console.error(`[ChannelBridge] Failed to fetch channels for ${guildId}:`, error);
                return [];
            } finally {
                // Clear pending request
                delete this._pendingRequests.channels[guildId];
            }
        })();
        
        return this._pendingRequests.channels[guildId];
    }
    
    /**
     * Get emojis for a guild via NEO token
     * @param {string} guildId - Discord guild ID
     * @returns {Promise<Array>} List of emojis
     */
    async getEmojis(guildId) {
        if (!guildId) return [];
        
        const cached = this._getCache('emojis', guildId);
        if (cached) {
            console.log(`[ChannelBridge] Using cached emojis for ${guildId}`);
            return cached;
        }
        
        try {
            const response = await fetch(`${this.API_BASE}/api/discord/rs/emojis/${guildId}`);
            if (!response.ok) {
                throw new Error(`API error: ${response.status}`);
            }
            
            const data = await response.json();
            const emojis = data.emojis || [];
            
            this._setCache('emojis', guildId, emojis);
            console.log(`[ChannelBridge] Fetched ${emojis.length} emojis for ${guildId}`);
            
            return emojis;
        } catch (error) {
            console.error(`[ChannelBridge] Failed to fetch emojis for ${guildId}:`, error);
            return [];
        }
    }
    
    /**
     * Get messages from a channel via NEO token
     * @param {string} channelId - Discord channel ID
     * @param {Object} options - { limit, after }
     * @returns {Promise<Array>} List of messages
     */
    async getMessages(channelId, options = {}) {
        if (!channelId) return [];
        
        const { limit = 50, after = null, useCache = true } = options;
        const cacheKey = `${channelId}_${after || 'latest'}`;
        
        if (useCache) {
            const cached = this._getCache('messages', cacheKey);
            if (cached) {
                console.log(`[ChannelBridge] Using cached messages for ${channelId}`);
                return cached;
            }
        }
        
        try {
            let url = `${this.API_BASE}/api/discord/rs/messages/${channelId}?limit=${limit}`;
            if (after) {
                url += `&after=${after}`;
            }
            
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`API error: ${response.status}`);
            }
            
            const data = await response.json();
            const messages = data.messages || [];
            
            this._setCache('messages', cacheKey, messages);
            console.log(`[ChannelBridge] Fetched ${messages.length} messages from ${channelId}`);
            
            return messages;
        } catch (error) {
            console.error(`[ChannelBridge] Failed to fetch messages from ${channelId}:`, error);
            return [];
        }
    }
    
    /**
     * Get environment/token status
     * @returns {Promise<Object>} Token configuration status
     */
    async getEnvStatus() {
        const cached = this._getCache('envStatus');
        if (cached) {
            return cached;
        }
        
        try {
            const response = await fetch(`${this.API_BASE}/api/env_status`);
            if (!response.ok) {
                throw new Error(`API error: ${response.status}`);
            }
            
            const data = await response.json();
            this._setCache('envStatus', null, data);
            return data;
        } catch (error) {
            console.error('[ChannelBridge] Failed to fetch env status:', error);
            return { tokens: {} };
        }
    }
    
    /**
     * Get bot status (running/stopped)
     * @returns {Promise<Object>} Bot status info
     */
    async getBotStatus() {
        // Check if request already in flight (request deduplication)
        if (this._pendingRequests.botStatus) {
            console.log('[ChannelBridge] Waiting for pending bot status request...');
            return this._pendingRequests.botStatus;
        }
        
        // Create new request and store promise
        this._pendingRequests.botStatus = (async () => {
            try {
                const response = await fetch(`${this.API_BASE}/api/bot_status`);
                if (!response.ok) {
                    throw new Error(`API error: ${response.status}`);
                }
                return await response.json();
            } catch (error) {
                console.error('[ChannelBridge] Failed to fetch bot status:', error);
                return { bots: {} };
            } finally {
                // Clear pending request after 2 seconds (allow quick re-check)
                setTimeout(() => { this._pendingRequests.botStatus = null; }, 2000);
            }
        })();
        
        return this._pendingRequests.botStatus;
    }
    
    // ========================================================================
    // CHANNEL MAPPING METHODS
    // ========================================================================
    
    async initialize() {
        await this.loadChannelMap();
        await this.loadServers();
        this.setupEventListeners();
    }
    
    setupEventListeners() {
        const sourceServerSelect = document.getElementById('bridge-source-server');
        const destServerSelect = document.getElementById('bridge-dest-server');
        const refreshBtn = document.getElementById('bridge-refresh-btn');
        const clearBtn = document.getElementById('bridge-clear-btn');

        if (sourceServerSelect) {
            sourceServerSelect.addEventListener('change', (e) => {
                this.sourceServerId = e.target.value;
                this.loadSourceChannels();
            });
        }

        if (destServerSelect) {
            destServerSelect.addEventListener('change', (e) => {
                this.destServerId = e.target.value;
                this.loadDestChannels();
            });
        }

        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.refresh());
        }

        if (clearBtn) {
            clearBtn.addEventListener('click', () => this.clearSelection());
        }
    }
    
    async loadChannelMap() {
        try {
            const response = await fetch(`${this.API_BASE}/config/channel_map.json`);
            if (response.ok) {
                this.channelMap = await response.json();
            }
        } catch (error) {
            console.log('[ChannelBridge] No existing channel map');
        }
    }
    
    async loadServers() {
        const guilds = await this.getGuilds();
        
        const sourceSelect = document.getElementById('bridge-source-server');
        const destSelect = document.getElementById('bridge-dest-server');

        if (sourceSelect) {
            sourceSelect.innerHTML = '<option value="">Select source server...</option>' +
                guilds.map(g => `<option value="${g.id}">${g.name}</option>`).join('');
        }

        if (destSelect) {
            destSelect.innerHTML = '<option value="">Select destination server...</option>' +
                guilds.map(g => `<option value="${g.id}">${g.name}</option>`).join('');
        }
    }
    
    async loadSourceChannels() {
        if (!this.sourceServerId) return;

        const channelsList = document.getElementById('bridge-source-channels');
        if (!channelsList) return;

        channelsList.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>Loading...</p></div>';

        this.sourceChannels = await this.getChannels(this.sourceServerId);
        this.renderSourceChannels();
    }
    
    async loadDestChannels() {
        if (!this.destServerId) return;

        const channelsList = document.getElementById('bridge-dest-channels');
        if (!channelsList) return;

        channelsList.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>Loading...</p></div>';

        this.destChannels = await this.getChannels(this.destServerId);
        this.renderDestChannels();
    }
    
    renderSourceChannels() {
        const channelsList = document.getElementById('bridge-source-channels');
        if (!channelsList) return;

        const textChannels = this.sourceChannels.filter(c => c.type === 0);
        
        if (textChannels.length === 0) {
            channelsList.innerHTML = '<div class="empty-state"><p>No text channels</p></div>';
            return;
        }

        channelsList.innerHTML = textChannels.map(channel => {
            const isSelected = this.selectedSourceChannels.has(channel.id);
            const isMapped = this.isChannelMapped(channel.id);
            return `
                <div class="channel-item ${isSelected ? 'selected' : ''} ${isMapped ? 'mapped' : ''}" 
                     onclick="channelBridge.toggleSourceChannel('${channel.id}')">
                    <span class="channel-icon">💬</span>
                    <span class="channel-name">${channel.name}</span>
                    ${channel.parent_name ? `<span class="channel-category">${channel.parent_name}</span>` : ''}
                    ${isMapped ? '<span style="color:var(--green);font-size:10px;margin-left:auto;">✓</span>' : ''}
                </div>
            `;
        }).join('');
    }
    
    renderDestChannels() {
        const channelsList = document.getElementById('bridge-dest-channels');
        if (!channelsList) return;

        const textChannels = this.destChannels.filter(c => c.type === 0);
        
        if (textChannels.length === 0) {
            channelsList.innerHTML = '<div class="empty-state"><p>No text channels</p></div>';
            return;
        }

        channelsList.innerHTML = textChannels.map(channel => {
            const sourceCount = this.getSourceCountForDest(channel.id);
            return `
                <div class="channel-item" onclick="channelBridge.createMapping('${channel.id}')">
                    <span class="channel-icon">💬</span>
                    <span class="channel-name">${channel.name}</span>
                    ${channel.parent_name ? `<span class="channel-category">${channel.parent_name}</span>` : ''}
                    ${sourceCount > 0 ? `<span style="color:var(--accent);font-size:10px;margin-left:auto;">${sourceCount}</span>` : ''}
                </div>
            `;
        }).join('');
    }
    
    toggleSourceChannel(channelId) {
        if (this.selectedSourceChannels.has(channelId)) {
            this.selectedSourceChannels.delete(channelId);
        } else {
            this.selectedSourceChannels.add(channelId);
        }
        this.renderSourceChannels();
    }
    
    isChannelMapped(channelId) {
        for (const destId in this.channelMap) {
            const sources = Array.isArray(this.channelMap[destId]) 
                ? this.channelMap[destId] 
                : [this.channelMap[destId]];
            if (sources.includes(channelId)) return true;
        }
        return false;
    }
    
    getSourceCountForDest(destId) {
        const sources = this.channelMap[destId];
        if (!sources) return 0;
        return Array.isArray(sources) ? sources.length : 1;
    }
    
    async createMapping(destChannelId) {
        if (this.selectedSourceChannels.size === 0) {
            alert('Select source channel(s) first');
            return;
        }

        const sources = Array.from(this.selectedSourceChannels);
        
        if (!this.channelMap[destChannelId]) {
            this.channelMap[destChannelId] = [];
        }
        
        const existing = Array.isArray(this.channelMap[destChannelId]) 
            ? this.channelMap[destChannelId] 
            : [this.channelMap[destChannelId]];
        
        sources.forEach(srcId => {
            if (!existing.includes(srcId)) {
                existing.push(srcId);
            }
        });
        
        this.channelMap[destChannelId] = existing;
        await this.saveChannelMap();
        this.clearMappingSelection();
        this.renderDestChannels();
        this.renderMappings();
    }
    
    async removeMapping(destChannelId, sourceChannelId) {
        if (this.channelMap[destChannelId]) {
            const sources = Array.isArray(this.channelMap[destChannelId])
                ? this.channelMap[destChannelId]
                : [this.channelMap[destChannelId]];
            
            const filtered = sources.filter(id => id !== sourceChannelId);
            
            if (filtered.length === 0) {
                delete this.channelMap[destChannelId];
            } else {
                this.channelMap[destChannelId] = filtered;
            }

            await this.saveChannelMap();
            this.renderMappings();
            this.renderSourceChannels();
        }
    }
    
    async saveChannelMap() {
        try {
            const response = await fetch(`${this.API_BASE}/api/channel_map`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(this.channelMap)
            });

            if (!response.ok) {
                throw new Error('Failed to save');
            }
            console.log('[ChannelBridge] Channel map saved');
        } catch (error) {
            console.error('[ChannelBridge] Save failed:', error);
            throw error;
        }
    }
    
    renderMappings() {
        const mappingsList = document.getElementById('bridge-mappings-list');
        if (!mappingsList) return;

        const entries = Object.entries(this.channelMap);
        
        if (entries.length === 0) {
            mappingsList.innerHTML = '<div class="empty-state"><div class="icon">🔗</div><p>No mappings</p></div>';
            return;
        }

        mappingsList.innerHTML = entries.map(([destId, sources]) => {
            const sourceArray = Array.isArray(sources) ? sources : [sources];
            const destChannel = this.destChannels.find(c => c.id === destId);
            const destName = destChannel?.name || destId;

            return `
                <div class="mapping-item">
                    <div class="mapping-dest">
                        <span>💬</span>
                        <span>${destName}</span>
                    </div>
                    <span class="mapping-arrow">←</span>
                    <div class="mapping-source" style="flex:2;">
                        ${sourceArray.map(srcId => {
                            const srcChannel = this.sourceChannels.find(c => c.id === srcId);
                            return `<span style="background:var(--bg-primary);padding:2px 6px;border-radius:4px;font-size:11px;margin-right:4px;">${srcChannel?.name || srcId}</span>`;
                        }).join('')}
                    </div>
                    <button class="mapping-remove" onclick="channelBridge.removeMapping('${destId}', '${sourceArray[0]}')" title="Remove">✕</button>
                </div>
            `;
        }).join('');
    }
    
    clearMappingSelection() {
        this.selectedSourceChannels.clear();
        this.renderSourceChannels();
    }
    
    async refresh() {
        this.clearCache();
        await this.loadChannelMap();
        await this.loadServers();
        if (this.sourceServerId) await this.loadSourceChannels();
        if (this.destServerId) await this.loadDestChannels();
        this.renderMappings();
    }
    
    async testMapping(destChannelId) {
        try {
            const response = await fetch(`${this.API_BASE}/api/channel_map/test`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ dest_channel_id: destChannelId })
            });
            
            const result = await response.json();
            alert(result.message || 'Test sent!');
        } catch (error) {
            alert('Test failed: ' + error.message);
        }
    }
}

// Create global instance
window.ChannelBridge = new ChannelBridgeAPI();

// Alias for backward compatibility
window.channelBridge = window.ChannelBridge;

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    // Only auto-initialize if we're on a page with bridge elements
    if (document.getElementById('bridge-source-server')) {
        window.ChannelBridge.initialize();
    }
});

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ChannelBridgeAPI;
}
