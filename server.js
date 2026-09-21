const { WebSocketServer } = require('ws');
const http = require('http');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');  // Import file system tools
const path = require('path');

// 1. DYNAMIC PORT: Use Render's assigned port OR fallback to 3000 locally
const PORT = process.env.PORT || 3000;

// 2. HTTP SERVER Upgrade: Automatically serve index.html to incoming visitors
const server = http.createServer((req, res) => {
    // If a user goes to your main URL page, read and send the index.html file
    if (req.url === '/' || req.url === '/index.html') {
        fs.readFile(path.join(__dirname, 'index.html'), (err, content) => {
            if (err) {
                res.writeHead(500);
                res.end('Error loading index.html');
            } else {
                res.writeHead(200, { 'Content-Type': 'text/html' });
                res.end(content, 'utf-8');
            }
        });
    } else {
        res.writeHead(404);
        res.end('Page Not Found');
    }
});

const wss = new WebSocketServer({ server });

// Store SQLite database in a persistent directory if available on Render
const dbPath = process.env.RENDER_DATA_DIR 
    ? path.join(process.env.RENDER_DATA_DIR, 'chat.db') 
    : './chat.db';

const db = new sqlite3.Database(dbPath, (err) => {
    if (err) console.error('Database connection error:', err.message);
    else console.log(`Connected to database at ${dbPath}`);
});

db.run(`
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        room TEXT NOT NULL,
        username TEXT NOT NULL,
        text TEXT NOT NULL,
        time TEXT NOT NULL
    )
`);

function broadcastRoomCounts() {
    const roomCounts = {};
    wss.clients.forEach((client) => {
        if (client.currentRoom) {
            roomCounts[client.currentRoom] = (roomCounts[client.currentRoom] || 0) + 1;
        }
    });

    const countPayload = JSON.stringify({
        type: 'room_counts',
        counts: roomCounts
    });
    
    wss.clients.forEach((client) => {
        if (client.readyState === 1) {
            client.send(countPayload);
        }
    });
}

wss.on('connection', (ws) => {
    console.log('A user connected.');
    ws.currentRoom = 'general';
    broadcastRoomCounts();

    ws.on('message', (bufferData) => {
        try {
            const rawMessage = bufferData.toString();
            const parsedData = JSON.parse(rawMessage);

            if (parsedData.type === 'join_room') {
                ws.currentRoom = parsedData.room;
                broadcastRoomCounts();

                db.all(
                    `SELECT username, text, time FROM messages WHERE room = ? ORDER BY id ASC LIMIT 50`,
                    [ws.currentRoom],
                    (err, rows) => {
                        if (err) return;
                        ws.send(JSON.stringify({
                            type: 'chat_history',
                            messages: rows
                        }));
                    }
                );
                return;
            }

            if (parsedData.type === 'chat_message') {
                const stmt = db.prepare(`INSERT INTO messages (room, username, text, time) VALUES (?, ?, ?, ?)`);
                stmt.run(ws.currentRoom, parsedData.username, parsedData.text, parsedData.time);
                stmt.finalize();
            }

            wss.clients.forEach((client) => {
                if (client !== ws && client.readyState === 1 && client.currentRoom === ws.currentRoom) {
                    client.send(rawMessage); 
                }
            });
        } catch (error) {
            console.error("Failed to process message:", error);
        }
    });

    ws.on('close', () => {
        console.log('A user disconnected.');
        broadcastRoomCounts();
    });
});

// Start listening on the dynamic production port
server.listen(PORT, () => {
    console.log(`[SUCCESS] Production-ready server running on port ${PORT}`);
});
