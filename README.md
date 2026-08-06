# ELD WebSocket Server

A real-time WebSocket server for the **ELD (Electronic Logging Device)** platform that provides instant communication between connected clients. It supports **real-time chat**, **force logout**, **change duty status**, **user lists**, **group messaging**, **message read status**, and **unread message counts**.

---

# Features

- 🔐 Token-based Authentication
- 💬 One-to-One Chat
- 👥 Group Chat
- 📨 Previous Message History
- 📊 Total Unread Message Count
- ✅ Read Status Updates
- 🚛 Real-time Duty Status Updates
- 🚪 Force Logout
- 👤 Driver List
- 👥 User List
- 📁 Group Management
- 🔄 Automatic Reconnection Support
- 📡 Broadcast Messages
- ⚡ High Performance WebSocket Communication

---

# Tech Stack

- Node.js
- WebSocket (`ws`)
- MySQL
- Axios
- Express HTTP Server
- REST API Authentication

---

# Project Structure

```
.
├── server.js
├── config/
├── database/
├── websocket/
├── helpers/
├── routes/
├── utils/
├── package.json
└── README.md
```

---

# Installation

Clone Repository

```bash
git clone <repository-url>
```

Install Packages

```bash
npm install
```

Start Server

```bash
npm start
```

Development

```bash
npm run dev
```

---

# Environment Variables

Create `.env`

```env
PORT=3001

BACKEND_URL=https://your-backend-url.com

DB_HOST=

DB_PORT=

DB_DATABASE=

DB_USERNAME=

DB_PASSWORD=
```

---

# WebSocket Connection

Client connects

```
ws://localhost:3001
```

or

```
wss://your-domain.com
```

---

# Authentication Flow

```
Client
   │
   │ Connect
   ▼
WebSocket Server
   │
   │ sendType : auth
   ▼
Laravel API
/check/user/access/token
   │
   ▼
Token Valid
   │
   ▼
User Authenticated
   │
   ▼
Socket Stored
```

Authentication Request

```json
{
  "sendType": "auth",
  "token": "ACCESS_TOKEN"
}
```

Authentication Success

```json
{
  "sendType": "auth_success",
  "authenticated": true,
  "user_id": 103
}
```

---

# Message Types

| sendType           | Description            |
| ------------------ | ---------------------- |
| auth               | Authenticate User      |
| message            | Send Chat Message      |
| totalMsg           | Get Total Unread Count |
| update_read_status | Mark Messages Read     |
| userInfo           | Load Users             |
| group_create       | Create Group           |
| change-duty-status | Driver Duty Status     |
| force_logout       | Logout User            |
| previous_messages  | Previous Chats         |

---

# Chat Flow

```
Client A
     │
     ▼
WebSocket Server
     │
     ▼
Store in Database
     │
     ▼
Client B
```

Private Message

```json
{
  "sendType": "message",
  "receiverId": 105,
  "message": "Hello",
  "type": 0
}
```

Group Message

```json
{
  "sendType": "message",
  "receiverId": 4,
  "message": "Hello Group",
  "type": 1
}
```

---

# Previous Messages

Request

```json
{
  "sendType": "message",
  "receiverId": 105
}
```

Server returns previous conversation after successful authentication.

---

# Total Unread Messages

Request

```json
{
  "sendType": "totalMsg"
}
```

Response

```json
{
  "sendType": "totalMsg",
  "totalUnread": 12
}
```

---

# Read Status

Request

```json
{
  "sendType": "update_read_status",
  "receiverId": 105,
  "isGroup": false
}
```

Response

```json
{
  "sendType": "message_read_status",
  "success": true
}
```

---

# User List

Request

```json
{
  "sendType": "userInfo"
}
```

Server sends

- User List
- Driver List
- Master List
- Group List

---

# Group Creation

Request

```json
{
  "sendType": "group_create",
  "group_name": "Support",
  "users": [101, 102, 103]
}
```

Response

```json
{
  "sendType": "group_list"
}
```

---

# Change Duty Status

This feature updates a driver's duty status in real time. Whenever a driver's duty status changes, the backend broadcasts the update to connected clients without requiring a page refresh.

Server Broadcast

```json
{
  "sendType": "change-duty-status",
  "driverId": 103,
  "status": "ON_DUTY"
}
```

Example Status Values

- OFF_DUTY
- ON_DUTY
- DRIVING
- SLEEPER_BERTH
- PERSONAL_USE
- YARD_MOVE

Client Example

```javascript
socket.onmessage = event => {
  const data = JSON.parse(event.data)

  if (data.sendType === 'change-duty-status') {
    console.log(data.status)
  }
}
```

---

# Force Logout

Force logout immediately disconnects a user's active WebSocket sessions. This is useful when an account is disabled, the access token expires, or an administrator invalidates the session.

Server Broadcast

```json
{
  "sendType": "force_logout",
  "message": "Your session has expired."
}
```

Client Example

```javascript
if (data.sendType === 'force_logout') {
  alert(data.message)
  socket.close()
}
```

---

# Broadcast Flow

```
Admin

   │

Change Duty

   │

WebSocket Server

   │

Broadcast

   │

All Connected Clients
```

---

# Error Response

```json
{
  "sendType": "error",
  "message": "Unknown sendType"
}
```

---

# Connection Lifecycle

```
Connect
    │
Authenticate
    │
Receive User Data
    │
Exchange Messages
    │
Receive Live Updates
    │
Disconnect
```

---

# Security

- JWT / Access Token Authentication
- Server-side User Validation
- Secure WebSocket (WSS)
- Token Verification using Backend API
- User ID derived from authenticated token
- Unauthorized requests rejected
- Multiple socket session management

---

# Performance

- Connection Pooling
- Real-time Broadcasting
- Efficient Message Routing
- Reduced API Calls
- Lightweight WebSocket Communication

---

# Future Enhancements

- Typing Indicators
- Message Reactions
- Push Notifications
- Voice Messages
- File Sharing
- Video Calling
- Presence Status
- Delivery Status

---

# Troubleshooting

## Connection Failed

- Verify WebSocket server is running.
- Check firewall and network access.
- Confirm the correct WebSocket URL.

## Authentication Failed

- Ensure the access token is valid.
- Verify the backend authentication API is reachable.

## Messages Not Received

- Confirm the recipient is connected.
- Check database connectivity.
- Review server logs for errors.

---

# License

Internal project for the ELD platform.

---

# Version

**v1.0.0**

---

# Author

**ELD Development Team**
