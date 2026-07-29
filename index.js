require('dotenv').config()

const http = require('http')
const cors = require('cors')
const axios = require('axios')
const WebSocket = require('ws')
const mysql = require('mysql2')
const express = require('express')
const moment = require('moment-timezone')
const { format, utcToZonedTime } = require('date-fns-tz')

const app = express()

const PORT = 3001

// ==========================================
// Express Middleware
// ==========================================

app.use(cors())
app.use(express.json())

// ==========================================
// MySQL Connection Pool
// ==========================================

const db = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 20,
  queueLimit: 20
})

// Test database connection
db.getConnection((err, connection) => {
  if (err) {
    console.error('Error connecting to the database:', err)
    process.exit(1)
  }

  console.log('Connected to the database!')

  connection.release()
})

// ==========================================
// WebSocket Server
// ==========================================

const wss = new WebSocket.Server({
  noServer: true
})

// ==========================================
// Helper Functions
// ==========================================

/**
 * Safely send JSON data to a WebSocket client
 */
function sendToClient(ws, data) {
  try {
    if (!ws) {
      return false
    }

    if (ws.readyState !== WebSocket.OPEN) {
      return false
    }

    ws.send(JSON.stringify(data))

    return true
  } catch (error) {
    console.error('WebSocket send error:', error)
    return false
  }
}

/**
 * Broadcast data to all connected clients
 */
function broadcast(data) {
  const message = JSON.stringify(data)

  wss.clients.forEach(client => {
    try {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message)
      }
    } catch (error) {
      console.error('Broadcast error:', error)
    }
  })
}

/**
 * Broadcast data only to a specific user
 */
function sendToUser(userId, data) {
  const targetUserId = Number(userId)

  let sent = false

  wss.clients.forEach(client => {
    try {
      if (
        client.readyState === WebSocket.OPEN &&
        Number(client.userId) === targetUserId
      ) {
        client.send(JSON.stringify(data))
        sent = true
      }
    } catch (error) {
      console.error('Send to user error:', error)
    }
  })

  return sent
}

// ==========================================
// WebSocket Connection
// ==========================================

wss.on('connection', (ws, req) => {
  console.log('New client connected')

  // Default user ID
  ws.userId = null

  // ========================================
  // WebSocket Message
  // ========================================

  ws.on('message', message => {
    try {
      // ======================================
      // IMPORTANT:
      // Convert Buffer to String
      // ======================================

      const rawMessage = message.toString().trim()

      // Ignore empty messages
      if (!rawMessage) {
        console.warn('Received empty WebSocket message. Ignoring.')
        return
      }

      console.log('Received WebSocket message:', rawMessage)

      // ======================================
      // Safely Parse JSON
      // ======================================

      let data

      try {
        data = JSON.parse(rawMessage)
      } catch (parseError) {
        console.error('Invalid JSON received from WebSocket client')
        console.error('Raw message:', JSON.stringify(rawMessage))
        console.error('Parse error:', parseError.message)

        sendToClient(ws, {
          type: 'error',
          sendType: 'invalid_json',
          message: 'Invalid JSON message received'
        })

        return
      }

      // Ensure parsed data is an object
      if (!data || typeof data !== 'object') {
        console.warn('Invalid WebSocket data format:', data)

        sendToClient(ws, {
          type: 'error',
          sendType: 'invalid_message',
          message: 'Message must be a valid JSON object'
        })

        return
      }

      console.log('Parsed WebSocket data:', data)

      // ======================================
      // AUTH
      // ======================================

      if (data.sendType === 'auth') {
        ws.userId = data.senderId

        const isGroup = data.isGroup ? 1 : 0

        const query = `
          SELECT
            um.*,
            u1.first_name AS sender_first_name,
            u1.last_name AS sender_last_name,
            u2.first_name AS reciever_first_name,
            u2.last_name AS reciever_last_name
          FROM user_message um
          LEFT JOIN users u1
            ON um.sender_id = u1.id
          LEFT JOIN users u2
            ON um.reciever_id = u2.id
          WHERE
            (
              (um.reciever_id = ? AND um.sender_id = ?)
              OR
              (um.group_id = ?)
              OR
              (um.reciever_id = ? AND um.sender_id = ?)
            )
            AND um.type = ?
          ORDER BY um.sent_time ASC
        `

        db.query(
          query,
          [
            data.recieverId,
            data.senderId,
            data.recieverId,
            data.senderId,
            data.recieverId,
            isGroup
          ],
          (err, results) => {
            if (err) {
              console.error(
                'Database error fetching messages:',
                err
              )
              return
            }

            results.forEach(msg => {
              const sender_id = msg.sender_id || null
              const reciever_id = msg.reciever_id || null

              const sender_name = `${msg.sender_first_name || 'Unknown'} ${
                msg.sender_last_name || ''
              }`.trim()

              const reciever_name = `${msg.reciever_first_name || 'Unknown'} ${
                msg.reciever_last_name || ''
              }`.trim()

              sendToClient(ws, {
                type: msg.type,
                sendType: 'previous_message',
                sender_id,
                sender_name,
                reciever_name,
                image_url: msg.image_url,
                receiver_id: reciever_id,
                content: msg.message_text,
                sent_time: msg.sent_time,
                id: msg.id,
                sender: data.senderId
              })
            })
          }
        )

        return
      }

      // ======================================
      // USER INFO
      // ======================================

      if (data.sendType === 'userInfo') {
        ws.userId = data.senderId

        const userQuery = `
          SELECT *
          FROM users
          WHERE master_id = ?
          ORDER BY created_at ASC
        `

        const driverQuery = `
          SELECT *
          FROM users
          WHERE master_id = ?
          AND id != ?
          ORDER BY created_at ASC
        `

        const masterQuery = `
          SELECT *
          FROM users
          WHERE id = ?
          AND user_type = 'TR'
          ORDER BY created_at ASC
        `

        const groupQuery = `
          SELECT
            g.group_id,
            g.group_name,
            g.created_by,
            g.created_at,
            ug.user_id AS user_group_user_id,
            u.first_name AS user_first_name,
            u.last_name AS user_last_name
          FROM groups g
          LEFT JOIN user_group ug
            ON g.group_id = ug.group_id
          LEFT JOIN users u
            ON ug.user_id = u.id
          WHERE g.created_by = ?
          ORDER BY g.group_id DESC
        `

        const userGroupQuery = `
          SELECT
            ug.*,
            g.*
          FROM user_group ug
          JOIN groups g
            ON ug.group_id = g.group_id
          WHERE ug.user_id = ?
          ORDER BY ug.id ASC
        `

        // Fetch users
        db.query(
          userQuery,
          [data.senderId],
          (err, userResults) => {
            if (err) {
              console.error(
                'Database error fetching users:',
                err
              )
              return
            }

            userResults.forEach(user => {
              sendToClient(ws, {
                id: user.id,
                first_name: user.first_name,
                last_name: user.last_name,
                email: user.email,
                type: 0,
                created_at: user.created_at,
                sendType: 'user_list',
                image_url: user.avatar_image
              })
            }

            // ==================================
            // Groups Created By User
            // ==================================

            db.query(
              groupQuery,
              [data.senderId],
              (err, groupResults) => {
                if (err) {
                  console.error(
                    'Database error fetching groups:',
                    err
                  )
                  return
                }

                const sentGroups = new Set()

                groupResults.forEach(group => {
                  if (
                    sentGroups.has(group.group_id)
                  ) {
                    return
                  }

                  sendToClient(ws, {
                    type: 1,
                    id: group.group_id,
                    group_name: group.group_name,
                    created_by: group.created_by,
                    created_at: group.created_at,
                    user_id: group.user_group_user_id,
                    user_name:
                      group.user_first_name &&
                      group.user_last_name
                        ? `${group.user_first_name} ${group.user_last_name}`
                        : null,
                    sendType: 'group_list'
                  })

                  sentGroups.add(group.group_id)
                })
              }
            )

            // ==================================
            // Drivers
            // ==================================

            db.query(
              driverQuery,
              [data.masterId, data.senderId],
              (err, driverData) => {
                if (err) {
                  console.error(
                    'Database error fetching driver data:',
                    err
                  )
                  return
                }

                const sentDrivers = new Set()

                driverData.forEach(user => {
                  if (
                    sentDrivers.has(user.id)
                  ) {
                    return
                  }

                  sendToClient(ws, {
                    id: user.id,
                    first_name: user.first_name,
                    last_name: user.last_name,
                    email: user.email,
                    type: 0,
                    created_at: user.created_at,
                    sendType: 'driver_list',
                    image_url: user.avatar_image
                  })

                  sentDrivers.add(user.id)
                })
              }
            )

            // ==================================
            // Master
            // ==================================

            db.query(
              masterQuery,
              [data.masterId],
              (err, masterData) => {
                if (err) {
                  console.error(
                    'Database error fetching master data:',
                    err
                  )
                  return
                }

                const sentMasters = new Set()

                masterData.forEach(user => {
                  if (
                    sentMasters.has(user.id)
                  ) {
                    return
                  }

                  sendToClient(ws, {
                    id: user.id,
                    first_name: user.first_name,
                    last_name: user.last_name,
                    email: user.email,
                    type: 0,
                    created_at: user.created_at,
                    sendType: 'master_list',
                    image_url: user.avatar_image
                  })

                  sentMasters.add(user.id)
                })
              }
            )

            // ==================================
            // User Groups
            // ==================================

            db.query(
              userGroupQuery,
              [data.senderId],
              (err, userGroupResults) => {
                if (err) {
                  console.error(
                    'Database error fetching user groups:',
                    err
                  )
                  return
                }

                const sentUserGroups = new Set()

                userGroupResults.forEach(
                  userGroup => {
                    if (
                      sentUserGroups.has(
                        userGroup.group_id
                      )
                    ) {
                      return
                    }

                    sendToClient(ws, {
                      type: 1,
                      id: userGroup.group_id,
                      group_name:
                        userGroup.group_name,
                      created_by:
                        userGroup.created_by,
                      created_at:
                        userGroup.created_at,
                      user_id:
                        userGroup.user_id,
                      sendType:
                        'user_group_list'
                    })

                    sentUserGroups.add(
                      userGroup.group_id
                    )
                  }
                )
              }
            )
          }
        )

        return
      }

      // ======================================
      // CREATE GROUP
      // ======================================

      if (data.sendType === 'group_create') {
        ws.userId = data.senderId

        const query = `
          INSERT INTO groups
          (
            group_name,
            master_id,
            master_company_id,
            created_by,
            is_active
          )
          VALUES (?, ?, ?, ?, ?)
        `

        db.query(
          query,
          [
            data.groupName,
            data.masterId,
            data.masterCompanyId,
            data.ids,
            1
          ],
          (err, result) => {
            if (err) {
              console.error(
                'Database error saving group:',
                err
              )
              return
            }

            const groupId = result.insertId

            const groupCreateQuery = `
              INSERT INTO user_group
              (
                group_id,
                user_id,
                is_active
              )
              VALUES (?, ?, ?)
            `

            const selectedUsers =
              Array.isArray(data.userSelected)
                ? data.userSelected
                : []

            selectedUsers.forEach(userId => {
              db.query(
                groupCreateQuery,
                [
                  groupId,
                  userId,
                  1
                ],
                err => {
                  if (err) {
                    console.error(
                      'Database error saving user-group association:',
                      err
                    )
                  }
                }
              )
            })

            broadcast({
              type: 1,
              group_id: groupId,
              sendType: 'group_list',
              group_name: data.groupName,
              user_id: selectedUsers,
              created_by: data.ids,
              created_at: ''
            })
          }
        )

        return
      }

      // ======================================
      // SEND MESSAGE
      // ======================================

      if (data.sendType === 'message') {
        if (data.type) {
          // Group message
          const query = `
            INSERT INTO user_message
            (
              type,
              sender_id,
              group_id,
              image_url,
              message_text,
              master_id,
              master_company_id,
              created_by,
              sent_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
          `

          db.query(
            query,
            [
              data.type,
              data.sender_id,
              data.reciever_id,
              data.image_url,
              data.content,
              data.master_id,
              data.master_company_id,
              data.sender_id,
              data.sent_time
            ],
            (err, result) => {
              if (err) {
                console.error(
                  'Database error saving group message:',
                  err
                )
                return
              }

              const senderQuery = `
                SELECT
                  first_name,
                  last_name
                FROM users
                WHERE id = ?
              `

              db.query(
                senderQuery,
                [data.sender_id],
                (err, senderResult) => {
                  if (err) {
                    console.error(
                      'Error fetching sender details:',
                      err
                    )
                    return
                  }

                  const sender_name =
                    senderResult.length > 0
                      ? `${senderResult[0].first_name || 'Unknown'} ${
                          senderResult[0].last_name || ''
                        }`.trim()
                      : 'Unknown'

                  broadcast({
                    type: data.type,
                    sendType: 'new_message',
                    reciever_id:
                      data.reciever_id,
                    sender_id:
                      data.sender_id,
                    sender_name,
                    reciever_name: null,
                    image_url:
                      data.image_url,
                    content:
                      data.content,
                    sent_time:
                      data.sent_time
                  })
                }
              )
            }
          )
        } else {
          // One-to-one message
          const query = `
            INSERT INTO user_message
            (
              type,
              sender_id,
              reciever_id,
              image_url,
              message_text,
              master_id,
              master_company_id,
              created_by,
              sent_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
          `

          db.query(
            query,
            [
              data.type,
              data.sender_id,
              data.reciever_id,
              data.image_url,
              data.content,
              data.master_id,
              data.master_company_id,
              data.sender_id,
              data.sent_time
            ],
            (err, result) => {
              if (err) {
                console.error(
                  'Database error saving message:',
                  err
                )
                return
              }

              const senderReceiverQuery = `
                SELECT
                  u1.first_name AS sender_first_name,
                  u1.last_name AS sender_last_name,
                  u2.first_name AS reciever_first_name,
                  u2.last_name AS reciever_last_name
                FROM users u1
                LEFT JOIN users u2
                  ON u2.id = ?
                WHERE u1.id = ?
              `

              db.query(
                senderReceiverQuery,
                [
                  data.reciever_id,
                  data.sender_id
                ],
                (err, namesResult) => {
                  if (err) {
                    console.error(
                      'Error fetching sender/receiver names:',
                      err
                    )
                    return
                  }

                  const sender_name =
                    namesResult.length > 0
                      ? `${namesResult[0].sender_first_name || 'Unknown'} ${
                          namesResult[0].sender_last_name || ''
                        }`.trim()
                      : 'Unknown'

                  const reciever_name =
                    namesResult.length > 0
                      ? `${namesResult[0].reciever_first_name || 'Unknown'} ${
                          namesResult[0].reciever_last_name || ''
                        }`.trim()
                      : 'Unknown'

                  broadcast({
                    type: data.type,
                    sendType:
                      'new_message',
                    reciever_id:
                      data.reciever_id,
                    sender_id:
                      data.sender_id,
                    sender_name,
                    reciever_name,
                    image_url:
                      data.image_url,
                    content:
                      data.content,
                    sent_time:
                      data.sent_time
                  })
                }
              )
            }
          )
        }

        return
      }

      // ======================================
      // TOTAL MESSAGES
      // ======================================

      if (data.sendType === 'totalMsg') {
        ws.userId = data.senderId

        const query = `
          SELECT *
          FROM user_message
          WHERE
            (
              (
                reciever_id = ?
                AND reciever_id != 0
                AND is_read = 0
              )
              OR
              (reciever_id = 0)
            )
          ORDER BY sent_time DESC
        `

        db.query(
          query,
          [data.receiverId],
          (err, results) => {
            if (err) {
              console.error(
                'Database error fetching messages:',
                err
              )
              return
            }

            const senderReceiverQuery = `
              SELECT
                u1.first_name AS sender_first_name,
                u1.last_name AS sender_last_name,
                u2.first_name AS reciever_first_name,
                u2.last_name AS reciever_last_name
              FROM users u1
              LEFT JOIN users u2
                ON u2.id = ?
              WHERE u1.id = ?
            `

            results.forEach(msg => {
              db.query(
                senderReceiverQuery,
                [
                  msg.reciever_id,
                  msg.sender_id
                ],
                (err, namesResult) => {
                  if (err) {
                    console.error(
                      'Error fetching sender/receiver names:',
                      err
                    )
                    return
                  }

                  const indiaTime = moment
                    .tz(
                      msg.sent_time,
                      'Asia/Kolkata'
                    )
                    .format(
                      'YYYY-MM-DD HH:mm:ss'
                    )

                  const sender_name =
                    namesResult.length > 0
                      ? `${namesResult[0].sender_first_name || 'Unknown'} ${
                          namesResult[0].sender_last_name || ''
                        }`.trim()
                      : 'Unknown'

                  const reciever_name =
                    namesResult.length > 0
                      ? `${namesResult[0].reciever_first_name || 'Unknown'} ${
                          namesResult[0].reciever_last_name || ''
                        }`.trim()
                      : 'Unknown'

                  sendToClient(ws, {
                    id: msg.id,
                    type: msg.type,
                    sendType: 'totalMsg',
                    is_read: msg.is_read,
                    sender:
                      data.senderId,
                    group_id:
                      msg.group_id,
                    sent_time:
                      indiaTime,
                    sender_id:
                      msg.sender_id,
                    image_url:
                      msg.image_url,
                    sender_name,
                    content:
                      msg.message_text,
                    reciever_name,
                    receiver_id:
                      msg.reciever_id
                        ? msg.reciever_id
                        : msg.group_id
                  })
                }
              )
            })
          }
        )

        return
      }

      // ======================================
      // UPDATE READ STATUS
      // ======================================

      if (
        data.sendType ===
        'update_read_status'
      ) {
        const updateQuery = `
          UPDATE user_message
          SET is_read = 1
          WHERE
            (
              group_id = 0
              AND reciever_id = ?
              AND sender_id = ?
            )
            OR
            (
              reciever_id = 0
              AND sender_id = ?
              AND group_id = ?
            )
        `

        if (data.isGroup) {
          const groupUpdateQuery = `
            UPDATE user_message
            SET is_read =
              CASE
                WHEN is_read = '0'
                  THEN ?
                WHEN FIND_IN_SET(?, is_read) = 0
                  THEN CONCAT(is_read, ',', ?)
                ELSE is_read
              END
            WHERE
              reciever_id = 0
              AND sender_id != ?
              AND group_id = ?
          `

          const readId = data.useType
            ? data.recieverId
            : data.id

          db.query(
            groupUpdateQuery,
            [
              readId,
              readId,
              readId,
              readId,
              data.senderId
            ],
            (err, updateResults) => {
              if (err) {
                console.error(
                  'Database error updating group messages:',
                  err
                )

                sendToClient(ws, {
                  type: 'error',
                  sendType:
                    'update_read_status',
                  message:
                    'Failed to update read status'
                })

                return
              }

              broadcast({
                type: 1,
                sendType:
                  'message_read_status',
                reciever_id:
                  data.recieverId,
                sender_id:
                  data.senderId,
                id: ws.userId,
                sent_time:
                  data.sent_time
              })
            }
          )
        } else {
          db.query(
            updateQuery,
            [
              data.recieverId,
              data.senderId,
              data.senderId,
              data.recieverId
            ],
            (err, updateResults) => {
              if (err) {
                console.error(
                  'Database error updating messages:',
                  err
                )

                sendToClient(ws, {
                  type: 'error',
                  sendType:
                    'update_read_status',
                  message:
                    'Failed to update read status'
                })

                return
              }

              broadcast({
                type: 0,
                sendType:
                  'message_read_status',
                reciever_id:
                  data.recieverId,
                sender_id:
                  data.senderId,
                sent_time:
                  data.sent_time
              })
            }
          )
        }

        return
      }

      // ======================================
      // UNKNOWN SEND TYPE
      // ======================================

      console.warn(
        'Unknown WebSocket sendType:',
        data.sendType
      )

      sendToClient(ws, {
        type: 'error',
        sendType: 'unknown_send_type',
        message: `Unknown sendType: ${data.sendType || 'undefined'}`
      })
    } catch (err) {
      console.error(
        'Unexpected WebSocket message error:',
        err
      )
    }
  })

  // ========================================
  // WebSocket Close
  // ========================================

  ws.on('close', (code, reason) => {
    console.log(
      `Client disconnected. User ID: ${ws.userId}, Code: ${code}, Reason: ${reason.toString()}`
    )
  })

  // ========================================
  // WebSocket Error
  // ========================================

  ws.on('error', error => {
    console.error(
      `WebSocket error for user ${ws.userId}:`,
      error
    )
  })
})

// ==========================================
// Health Check
// ==========================================

app.get('/', (req, res) => {
  res.status(200).json({
    status: 'success',
    message: 'WebSocket server is running',
    port: PORT,
    connectedClients: wss.clients.size
  })
})

// ==========================================
// Broadcast Duty Status
// ==========================================

app.post(
  '/broadcast-duty-status',
  (req, res) => {
    try {
      const data = req.body

      const driverId = Number(
        data.driverId
      )

      if (!driverId) {
        return res.status(400).json({
          status: 'failure',
          message:
            'Valid driverId is required'
        })
      }

      const sent = sendToUser(
        driverId,
        {
          sendType:
            'change-duty-status',
          driverId,
          driver: data.driver,
          vehicle: data.vehicle,
          shiftStatus:
            data.shiftStatus,
          startLogTime:
            data.startLogTime,
          endLogTime:
            data.endLogTime,
          duration:
            data.duration,
          locationName:
            data.locationName,
          shift_time:
            data.shift_time,
          cycle_time:
            data.cycle_time,
          break_time:
            data.break_time,
          drive_time:
            data.drive_time,
          odometer:
            data.odometer,
          engineHours:
            data.engineHours
        }
      )

      console.log(
        sent
          ? `Duty status sent to user ID: ${driverId}`
          : `Driver ${driverId} is not connected`
      )

      return res.status(200).json({
        status: 'success',
        message: sent
          ? 'Duty status sent successfully'
          : 'Driver is not connected',
        driverId
      })
    } catch (error) {
      console.error(
        'Error broadcasting duty status:',
        error
      )

      return res.status(500).json({
        status: 'failure',
        message:
          'Failed to broadcast duty status'
      })
    }
  }
)

// ==========================================
// Create HTTP Server
// ==========================================

const server = http.createServer(app)

// ==========================================
// WebSocket Upgrade
// ==========================================

server.on(
  'upgrade',
  (request, socket, head) => {
    try {
      console.log(
        `WebSocket upgrade request: ${request.url}`
      )

      wss.handleUpgrade(
        request,
        socket,
        head,
        ws => {
          wss.emit(
            'connection',
            ws,
            request
          )
        }
      )
    } catch (error) {
      console.error(
        'WebSocket upgrade error:',
        error
      )

      socket.destroy()
    }
  }
)

// ==========================================
// Start Server
// ==========================================

server.listen(PORT, '0.0.0.0', () => {
  console.log(
    `HTTP + WebSocket server running on port ${PORT}`
  )
})
