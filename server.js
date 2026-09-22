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
    // Upgraded users table structure containing avatar mapping values
    db.run(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            avatar TEXT DEFAULT '👤'
        )
    `);

    db.run(`
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room TEXT NOT NULL,
            username TEXT NOT NULL,
            text TEXT NOT NULL,
            time TEXT NOT NULL,
            avatar TEXT DEFAULT '👤'
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
                const userAvatar = avatar || '👤';

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
            if (err) { res.writeHead(500); res.end('Error loading client file'); }
            else { res.writeHead(200, { 'Content-Type': 'text/html' }); res.end(content, 'utf-8'); }
        });
    } else {
        res.writeHead(404); res.end('Not Found');
    }
});

const wss = new WebSocketServer({ server });

function broadcastActiveState() {
    const activeUsers = [];
    const roomCounts = {};

    wss.clients.forEach(c => {
        if (c.username) {
            activeUsers.push({ username: c.username, avatar: c.avatar || '👤', status: 'online' });
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
    ws.avatar = '👤';

    ws.on('message', (bufferData) => {
        try {
            const rawMessage = bufferData.toString();
            const parsedData = JSON.parse(rawMessage);

            if (parsedData.type === 'init') {
                ws.username = parsedData.username;
                ws.avatar = parsedData.avatar || '👤';
                broadcastActiveState();
                return;
            }

            if (parsedData.type === 'join_room') {
                ws.currentRoom = parsedData.room;
                broadcastActiveState();
                
                db.all(`SELECT username, text, time, avatar FROM messages WHERE room = ? ORDER BY id ASC LIMIT 50`, [ws.currentRoom], (err, rows) => {
                    if (!err) ws.send(JSON.stringify({ type: 'chat_history', messages: rows }));
                });
                return;
            }

            if (parsedData.type === 'chat_message') {
                const stmt = db.prepare(`INSERT INTO messages (room, username, text, time, avatar) VALUES (?, ?, ?, ?, ?)`);
                stmt.run(ws.currentRoom, parsedData.username, parsedData.text, parsedData.time, parsedData.avatar);
                stmt.finalize();
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

server.listen(PORT, () => console.log(`Auth, DM & Profile server active on port ${PORT}`));
