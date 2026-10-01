const { WebSocketServer } = require('ws');
const http = require('http');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');

const PORT = process.env.PORT || 3000;

const dbPath = process.env.RENDER_DATA_DIR 
    ? path.join(process.env.RENDER_DATA_DIR, 'chat.db') 
    : './chat.db';

const db = new sqlite3.Database(dbPath, (err) => {
    if (err) console.error('Database connection error:', err.message);
});

db.serialize(() => {
    db.run(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            avatar TEXT DEFAULT '🦊'
        )
    `);

    db.run(`
        CREATE TABLE IF NOT EXISTS messages (
            message_id TEXT PRIMARY KEY,
            room TEXT NOT NULL,
            username TEXT NOT NULL,
            text TEXT NOT NULL,
            time TEXT NOT NULL,
            avatar TEXT DEFAULT '🦊',
            reply_to_id TEXT DEFAULT NULL,
            reply_to_user TEXT DEFAULT NULL,
            reply_to_text TEXT DEFAULT NULL
        )
    `);

    // Safely add missing columns if upgrading an existing database
    const addColumnSafely = (columnDef) => {
        db.run(`ALTER TABLE messages ADD COLUMN ${columnDef}`, (err) => {
            // Ignore error if column already exists
        });
    };

    addColumnSafely('reply_to_id TEXT DEFAULT NULL');
    addColumnSafely('reply_to_user TEXT DEFAULT NULL');
    addColumnSafely('reply_to_text TEXT DEFAULT NULL');

    db.run(`
        CREATE TABLE IF NOT EXISTS reactions (
            message_id TEXT NOT NULL,
            username TEXT NOT NULL,
            emoji TEXT NOT NULL,
            PRIMARY KEY (message_id, username, emoji)
        )
    `);
});

const server = http.createServer((req, res) => {
    const sendJSON = (status, obj) => {
        res.writeHead(status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(obj));
    };

    if (req.method === 'POST' && req.url === '/register') {
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', () => {
            try {
                const { username, password, avatar } = JSON.parse(body);
                if (!username || !password) return sendJSON(400, { error: 'Missing fields' });

                const hashedPassword = bcrypt.hashSync(password, 10);
                const userAvatar = avatar || '🦊';

                db.run(`INSERT INTO users (username, password, avatar) VALUES (?, ?, ?)`, 
                    [username, hashedPassword, userAvatar], function(err) {
                        if (err) return sendJSON(400, { error: 'Username already taken.' });
                        sendJSON(201, { success: 'User registered successfully!' });
                    }
                );
            } catch (e) { sendJSON(400, { error: 'Invalid payload' }); }
        });
        return;
    }

    if (req.method === 'POST' && req.url === '/login') {
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', () => {
            try {
                const { username, password } = JSON.parse(body);
                db.get(`SELECT * FROM users WHERE username = ?`, [username], (err, user) => {
                    if (err || !user) return sendJSON(401, { error: 'Invalid username or password' });

                    const passwordMatches = bcrypt.compareSync(password, user.password);
                    if (!passwordMatches) return sendJSON(401, { error: 'Invalid username or password' });

                    sendJSON(200, { username: user.username, avatar: user.avatar });
                });
            } catch (e) { sendJSON(400, { error: 'Invalid payload' }); }
        });
        return;
    }

    if (req.url === '/' || req.url === '/index.html') {
        fs.readFile(path.join(__dirname, 'index.html'), (err, content) => {
            if (err) { 
                res.writeHead(500); 
                res.end('Error loading client file'); 
            } else { 
                res.writeHead(200, { 'Content-Type': 'text/html' }); 
                res.end(content, 'utf-8'); // Fixed typo: utf-8
            }
        });
    } else {
        res.writeHead(404); 
        res.end('Not Found');
    }
});

const wss = new WebSocketServer({ server });

function broadcastActiveState() {
    const activeUsers = [];
    const roomCounts = {};

    wss.clients.forEach(c => {
        if (c.username) {
            activeUsers.push({ username: c.username, avatar: c.avatar || '🦊', status: 'online' });
        }
        if (c.currentRoom && !c.currentRoom.startsWith('dm_')) {
            roomCounts[c.currentRoom] = (roomCounts[c.currentRoom] || 0) + 1;
        }
    });

    const payload = JSON.stringify({ 
        type: 'state_update', 
        counts: roomCounts,
        users: activeUsers
    });

    wss.clients.forEach(c => { if (c.readyState === 1) c.send(payload); });
}

wss.on('connection', (ws) => {
    ws.currentRoom = 'general';
    ws.username = null;
    ws.avatar = '🦊';

    ws.on('message', (bufferData) => {
        try {
            const rawMessage = bufferData.toString();
            const parsedData = JSON.parse(rawMessage);

            if (parsedData.type === 'init') {
                ws.username = parsedData.username;
                ws.avatar = parsedData.avatar || '🦊';
                broadcastActiveState();
                return;
            }

            if (parsedData.type === 'join_room') {
                ws.currentRoom = parsedData.room;
                broadcastActiveState();
                
                const query = `
                    SELECT m.*, 
                           (SELECT json_group_array(json_object('emoji', r.emoji, 'username', r.username)) 
                            FROM reactions r WHERE r.message_id = m.message_id) as reactions
                    FROM messages m WHERE m.room = ? ORDER BY m.message_id ASC LIMIT 50
                `;

                db.all(query, [ws.currentRoom], (err, rows) => {
                    if (!err) {
                        const historyPayload = rows.map(row => ({
                            message_id: row.message_id,
                            username: row.username,
                            text: row.text,
                            time: row.time,
                            avatar: row.avatar,
                            reply_to_id: row.reply_to_id,
                            reply_to_user: row.reply_to_user,
                            reply_to_text: row.reply_to_text,
                            reactions: JSON.parse(row.reactions || '[]')
                        }));
                        ws.send(JSON.stringify({ type: 'chat_history', messages: historyPayload }));
                    }
                });
                return;
            }

            if (parsedData.type === 'chat_message') {
                const stmt = db.prepare(`
                    INSERT INTO messages (message_id, room, username, text, time, avatar, reply_to_id, reply_to_user, reply_to_text) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                `);
                stmt.run(
                    parsedData.message_id, 
                    ws.currentRoom, 
                    parsedData.username, 
                    parsedData.text, 
                    parsedData.time, 
                    parsedData.avatar,
                    parsedData.reply_to_id || null,
                    parsedData.reply_to_user || null,
                    parsedData.reply_to_text || null
                );
                stmt.finalize();
            }

            if (parsedData.type === 'toggle_reaction') {
                const checkQuery = `SELECT * FROM reactions WHERE message_id = ? AND username = ? AND emoji = ?`;
                db.get(checkQuery, [parsedData.message_id, parsedData.username, parsedData.emoji], (err, row) => {
                    if (row) {
                        db.run(`DELETE FROM reactions WHERE message_id = ? AND username = ? AND emoji = ?`, 
                            [parsedData.message_id, parsedData.username, parsedData.emoji]);
                    } else {
                        db.run(`INSERT INTO reactions (message_id, username, emoji) VALUES (?, ?, ?)`, 
                            [parsedData.message_id, parsedData.username, parsedData.emoji]);
                    }
                });
            }

            wss.clients.forEach(c => {
                if (c !== ws && c.readyState === 1 && c.currentRoom === ws.currentRoom) {
                    c.send(rawMessage); 
                }
            });
        } catch (error) { console.error(error); }
    });

    ws.on('close', () => { broadcastActiveState(); });
});

server.listen(PORT, () => console.log(`Auth, DM, Profile & Reactions server active on port ${PORT}`));
