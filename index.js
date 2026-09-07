require('dotenv').config()

const http = require('http')
const cors = require('cors')
const axios = require('axios')
const WebSocket = require('ws')
const mysql = require('mysql2')
const express = require('express')
const moment = require('moment-timezone')

const app = express()

const PORT = Number(process.env.PORT || 3001)

app.use(cors())
app.use(express.json())

// =====================================================
// MYSQL
// =====================================================

const db = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 20,
  queueLimit: 20
})

db.getConnection((err, connection) => {
  if (err) {
    console.error('Database connection failed:', err)
    process.exit(1)
  }

  console.log('MySQL connected successfully')

  connection.release()
})

const wss = new WebSocket.Server({
  noServer: true
})

function sendToClient (ws, data) {
  try {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return false
    }

    ws.send(JSON.stringify(data))

    return true
  } catch (error) {
    console.error('sendToClient error:', error)

    return false
  }
}

function sendToUser (userId, data) {
  const targetUserId = Number(userId)

  let sent = false

  wss.clients.forEach(client => {
    if (
      client.readyState === WebSocket.OPEN &&
      client.authenticated === true &&
      Number(client.userId) === targetUserId
    ) {
      sendToClient(client, data)

      sent = true
    }
  })

  return sent
}

function closeUserSockets (userId, reason = 'Session expired') {
  const targetUserId = Number(userId)

  let closed = false

  wss.clients.forEach(client => {
    if (
      client.readyState === WebSocket.OPEN &&
      client.authenticated === true &&
      Number(client.userId) === targetUserId
    ) {
      sendToClient(client, {
        type: 'error',
        sendType: 'auth_expired',
        authenticated: false,
        message: reason
      })

      client.authenticated = false

      // client.close(1008, reason)

      closed = true
    }
  })

  return closed
}

function isSocketAuthenticated (ws) {
  return Boolean(
    ws &&
      ws.readyState === WebSocket.OPEN &&
      ws.authenticated === true &&
      ws.userId
  )
}

function requireAuthentication (ws) {
  if (!isSocketAuthenticated(ws)) {
    sendToClient(ws, {
      type: 'error',
      sendType: 'auth_required',
      authenticated: false,
      message: 'Please authenticate WebSocket connection first'
    })

    return false
  }

  return true
}

function getUserName (userId, callback) {
  const query = `
    SELECT
      id,
      first_name,
      last_name
    FROM users
    WHERE id = ?
    LIMIT 1
  `

  db.query(query, [userId], (err, results) => {
    if (err) {
      console.error('getUserName error:', err)

      callback(null)

      return
    }

    if (!results.length) {
      callback(null)

      return
    }

    const user = results[0]

    callback({
      id: user.id,

      first_name: user.first_name || '',

      last_name: user.last_name || '',

      name: `${user.first_name || ''} ${user.last_name || ''}`.trim()
    })
  })
}

function normalizeAccessToken (token) {
  return String(token || '')
    .replace(/^Bearer\s+/i, '')
    .trim()
}

async function checkAccessToken (token) {
  try {
    const normalizedToken = normalizeAccessToken(token)

    if (!normalizedToken) {
      return {
        valid: false,
        statusCode: 401,
        message: 'Access token is required'
      }
    }

    const backendUrl = String(process.env.BACKEND_URL || '').replace(/\/$/, '')

    if (!backendUrl) {
      console.error('[AUTH] BACKEND_URL is not configured')

      return {
        valid: false,
        statusCode: 500,
        message: 'BACKEND_URL is not configured'
      }
    }

    const validationUrl = `${backendUrl}/check/user/access/token`

    console.log('----------------------------------------')
    console.log('[AUTH] Validating access token')
    console.log('[AUTH] Validation URL:', validationUrl)
    console.log('[AUTH] Token present:', true)
    console.log('[AUTH] Token length:', normalizedToken.length)
    console.log('----------------------------------------')

    const response = await axios.post(
      validationUrl,
      {},
      {
        headers: {
          Authorization: `Bearer ${normalizedToken}`,
          Accept: 'application/json',
          'Content-Type': 'application/json'
        },

        timeout: 10000,

        // IMPORTANT:
        // Allow axios to return 401 instead of throwing.
        validateStatus: () => true
      }
    )

    console.log('[AUTH] Laravel status:', response.status)

    console.log('[AUTH] Laravel success:', response.data?.success)

    console.log('[AUTH] Laravel message:', response.data?.message)

    if (response.status === 200 && response.data?.success === true) {
      const userId =
        response.data?.data?.user_id ??
        response.data?.data?.id ??
        response.data?.user_id ??
        response.data?.user?.id

      if (!userId) {
        console.error(
          '[AUTH] Token is valid but Laravel did not return user ID'
        )

        return {
          valid: false,
          statusCode: 200,
          message: 'Token is valid but Laravel did not return a user ID'
        }
      }

      return {
        valid: true,
        statusCode: response.status,
        userId: Number(userId)
      }
    }

    return {
      valid: false,

      statusCode: response.status,

      message:
        response.data?.message ||
        `Access token validation failed with status ${response.status}`
    }
  } catch (error) {
    console.error('[AUTH] Token validation request failed:', error.message)

    console.error('[AUTH] HTTP status:', error.response?.status || null)

    console.error('[AUTH] Laravel response:', error.response?.data || null)

    return {
      valid: false,

      statusCode: error.response?.status || 500,

      message:
        error.response?.data?.message ||
        error.message ||
        'Access token validation failed'
    }
  }
}

async function authenticateSocket (ws, data) {
  try {
    if (ws.authenticated === true) {
      sendToClient(ws, {
        type: 'success',

        sendType: 'auth_success',

        authenticated: true,

        user_id: ws.userId,

        message: 'WebSocket is already authenticated'
      })

      return true
    }

    const token = normalizeAccessToken(data?.token)

    if (!token) {
      console.warn('[AUTH] Authentication request received without token')

      sendToClient(ws, {
        type: 'error',

        sendType: 'auth_failed',

        authenticated: false,

        statusCode: 401,

        message: 'Access token is required'
      })

      ws.authenticated = false
      ws.userId = null
      ws.accessToken = null

      // IMPORTANT:
      // DO NOT CLOSE THE WEBSOCKET.
      //
      // The client can remain connected and decide
      // when/how to authenticate again.
      return false
    }

    const tokenResult = await checkAccessToken(token)

    // IMPORTANT:
    // Never print the actual access token.
    console.log('[AUTH] Token validation result:', {
      valid: tokenResult.valid,

      statusCode: tokenResult.statusCode || null,

      userId: tokenResult.userId || null,

      message: tokenResult.message || null
    })

    if (!tokenResult.valid) {
      sendToClient(ws, {
        type: 'error',

        sendType: 'auth_failed',

        authenticated: false,

        statusCode: tokenResult.statusCode || 401,

        message: tokenResult.message || 'Access token is invalid or expired'
      })

      ws.authenticated = false
      ws.userId = null
      ws.accessToken = null

      // IMPORTANT:
      // DO NOT CLOSE SOCKET HERE.
      return false
    }

    const authenticatedUserId = Number(tokenResult.userId)

    if (!authenticatedUserId) {
      sendToClient(ws, {
        type: 'error',

        sendType: 'auth_failed',

        authenticated: false,

        statusCode: 401,

        message: 'Unable to identify authenticated user'
      })

      ws.authenticated = false
      ws.userId = null
      ws.accessToken = null

      // IMPORTANT:
      // DO NOT CLOSE SOCKET HERE.
      return false
    }

    ws.userId = authenticatedUserId

    ws.accessToken = token

    ws.authenticated = true

    ws.authenticatedAt = new Date()

    sendToClient(ws, {
      type: 'success',

      sendType: 'auth_success',

      authenticated: true,

      user_id: ws.userId,

      message: 'Access token is valid and active'
    })

    console.log(
      `[AUTH] WebSocket authenticated successfully for user ${ws.userId}`
    )

    return true
  } catch (error) {
    console.error('[AUTH] authenticateSocket error:', error)

    ws.authenticated = false

    ws.userId = null

    ws.accessToken = null

    sendToClient(ws, {
      type: 'error',

      sendType: 'auth_failed',

      authenticated: false,

      statusCode: 500,

      message: 'Authentication failed'
    })

    // IMPORTANT:
    // DO NOT CLOSE SOCKET HERE.
    return false
  }
}

function getPreviousMessages (ws, senderId, receiverId, isGroup) {
  if (!isSocketAuthenticated(ws)) {
    return
  }

  let query

  let params

  if (isGroup) {
    query = `
      SELECT
        um.*,

        u1.first_name AS sender_first_name,
        u1.last_name AS sender_last_name

      FROM user_message um

      LEFT JOIN users u1
        ON um.sender_id = u1.id

      WHERE
        um.type = 1
        AND um.group_id = ?

      ORDER BY um.sent_time ASC
    `

    params = [receiverId]
  } else {
    query = `
      SELECT
        um.*,

        u1.first_name AS sender_first_name,
        u1.last_name AS sender_last_name,

        u2.first_name AS receiver_first_name,
        u2.last_name AS receiver_last_name

      FROM user_message um

      LEFT JOIN users u1
        ON um.sender_id = u1.id

      LEFT JOIN users u2
        ON um.reciever_id = u2.id

      WHERE
        um.type = 0
        AND
        (
          (
            um.sender_id = ?
            AND um.reciever_id = ?
          )
          OR
          (
            um.sender_id = ?
            AND um.reciever_id = ?
          )
        )

      ORDER BY um.sent_time ASC
    `

    params = [senderId, receiverId, receiverId, senderId]
  }

  db.query(query, params, (err, results) => {
    if (err) {
      console.error('Error fetching previous messages:', err)

      sendToClient(ws, {
        type: 'error',

        sendType: 'previous_message_error',

        message: 'Failed to load previous messages'
      })

      return
    }

    results.forEach(msg => {
      const senderName = `${msg.sender_first_name || 'Unknown'} ${
        msg.sender_last_name || ''
      }`.trim()

      const receiverName = `${msg.receiver_first_name || 'Unknown'} ${
        msg.receiver_last_name || ''
      }`.trim()

      sendToClient(ws, {
        id: msg.id,

        type: Number(msg.type),

        sendType: 'previous_message',

        sender_id: Number(msg.sender_id),

        receiver_id: Number(msg.reciever_id || 0),

        reciever_id: Number(msg.reciever_id || 0),

        group_id: Number(msg.group_id || 0),

        sender_name: senderName,

        receiver_name: receiverName,

        reciever_name: receiverName,

        content: msg.message_text,

        image_url: msg.image_url || null,

        sent_time: msg.sent_time,

        is_read: msg.is_read,

        sender: Number(msg.sender_id)
      })
    })

    sendToClient(ws, {
      type: 'success',

      sendType: 'previous_messages_loaded',

      receiver_id: receiverId,

      group_id: isGroup ? receiverId : 0,

      isGroup,

      total: results.length
    })
  })
}

function sendUserInfo (ws, masterId) {
  const senderId = Number(ws.userId)

  if (!senderId) {
    return
  }

  sendToClient(ws, {
    sendType: 'user_list_start'
  })

  const userQuery = `
    SELECT
      id,
      first_name,
      last_name,
      email,
      created_at,
      avatar_image

    FROM users

    WHERE master_id = ?

    ORDER BY created_at ASC
  `

  db.query(userQuery, [senderId], (err, users) => {
    if (err) {
      console.error('User list error:', err)

      return
    }

    users.forEach(user => {
      sendToClient(ws, {
        id: user.id,

        first_name: user.first_name,

        last_name: user.last_name,

        email: user.email,

        type: 0,

        created_at: user.created_at,

        image_url: user.avatar_image,

        sendType: 'user_list'
      })
    })
  })

  // ===================================================
  // DRIVERS
  // ===================================================

  const driverQuery = `
    SELECT
      id,
      first_name,
      last_name,
      email,
      created_at,
      avatar_image

    FROM users

    WHERE master_id = ?

    AND id != ?

    ORDER BY created_at ASC
  `

  db.query(driverQuery, [masterId, senderId], (err, drivers) => {
    if (err) {
      console.error('Driver list error:', err)

      return
    }

    drivers.forEach(user => {
      sendToClient(ws, {
        id: user.id,

        first_name: user.first_name,

        last_name: user.last_name,

        email: user.email,

        type: 0,

        created_at: user.created_at,

        image_url: user.avatar_image,

        sendType: 'driver_list'
      })
    })
  })

  // ===================================================
  // MASTER
  // ===================================================

  const masterQuery = `
    SELECT
      id,
      first_name,
      last_name,
      email,
      created_at,
      avatar_image

    FROM users

    WHERE id = ?

    AND user_type = 'TR'

    LIMIT 1
  `

  db.query(masterQuery, [masterId], (err, masters) => {
    if (err) {
      console.error('Master list error:', err)

      return
    }

    masters.forEach(user => {
      sendToClient(ws, {
        id: user.id,

        first_name: user.first_name,

        last_name: user.last_name,

        email: user.email,

        type: 0,

        created_at: user.created_at,

        image_url: user.avatar_image,

        sendType: 'master_list'
      })
    })
  })

  // ===================================================
  // GROUPS
  // ===================================================

  const groupQuery = `
    SELECT DISTINCT
      g.group_id,
      g.group_name,
      g.created_by,
      g.created_at

    FROM groups g

    LEFT JOIN user_group ug
      ON g.group_id = ug.group_id

    WHERE
      g.created_by = ?

      OR ug.user_id = ?

    ORDER BY g.group_id DESC
  `

  db.query(groupQuery, [senderId, senderId], (err, groups) => {
    if (err) {
      console.error('Group list error:', err)

      return
    }

    groups.forEach(group => {
      sendToClient(ws, {
        id: group.group_id,

        group_id: group.group_id,

        type: 1,

        group_name: group.group_name,

        created_by: group.created_by,

        created_at: group.created_at,

        sendType: 'group_list'
      })
    })
  })
}

// =====================================================
// TOTAL UNREAD MESSAGES
// =====================================================

function sendTotalUnreadMessages (ws) {
  if (!isSocketAuthenticated(ws)) {
    return
  }

  const userId = Number(ws.userId)

  if (!userId) {
    return
  }

  // ===================================================
  // ONE-TO-ONE UNREAD
  // ===================================================

  const oneToOneQuery = `
    SELECT
      um.*,

      u1.first_name AS sender_first_name,
      u1.last_name AS sender_last_name,

      u2.first_name AS receiver_first_name,
      u2.last_name AS receiver_last_name

    FROM user_message um

    LEFT JOIN users u1
      ON um.sender_id = u1.id

    LEFT JOIN users u2
      ON um.reciever_id = u2.id

    WHERE
      um.type = 0

      AND um.reciever_id = ?

      AND (
        um.is_read = 0
        OR um.is_read IS NULL
      )

    ORDER BY um.sent_time ASC
  `

  db.query(oneToOneQuery, [userId], (err, messages) => {
    if (err) {
      console.error('Unread one-to-one messages error:', err)

      sendToClient(ws, {
        type: 'error',

        sendType: 'total_message_error',

        message: 'Failed to load unread messages'
      })

      return
    }

    messages.forEach(msg => {
      const senderName = `${msg.sender_first_name || ''} ${
        msg.sender_last_name || ''
      }`.trim()

      const receiverName = `${msg.receiver_first_name || ''} ${
        msg.receiver_last_name || ''
      }`.trim()

      sendToClient(ws, {
        id: msg.id,

        type: 0,

        sendType: 'totalMsg',

        sender_id: Number(msg.sender_id),

        receiver_id: Number(msg.reciever_id || 0),

        reciever_id: Number(msg.reciever_id || 0),

        group_id: 0,

        sender_name: senderName || 'Unknown',

        receiver_name: receiverName || 'Unknown',

        reciever_name: receiverName || 'Unknown',

        content: msg.message_text,

        image_url: msg.image_url || null,

        sent_time: msg.sent_time,

        is_read: msg.is_read,

        sender: Number(msg.sender_id)
      })
    })
  })

  // ===================================================
  // GROUP UNREAD
  // ===================================================

  const groupQuery = `
    SELECT DISTINCT
      um.*,

      u1.first_name AS sender_first_name,
      u1.last_name AS sender_last_name,

      g.group_name

    FROM user_message um

    INNER JOIN user_group ug
      ON um.group_id = ug.group_id

    LEFT JOIN users u1
      ON um.sender_id = u1.id

    LEFT JOIN groups g
      ON um.group_id = g.group_id

    WHERE
      um.type = 1

      AND ug.user_id = ?

      AND ug.is_active = 1

      AND um.sender_id != ?

      AND (
        um.is_read = 0
        OR um.is_read IS NULL
      )

    ORDER BY um.sent_time ASC
  `

  db.query(groupQuery, [userId, userId], (err, messages) => {
    if (err) {
      console.error('Unread group messages error:', err)

      sendToClient(ws, {
        type: 'error',

        sendType: 'total_message_error',

        message: 'Failed to load unread group messages'
      })

      return
    }

    messages.forEach(msg => {
      const senderName = `${msg.sender_first_name || ''} ${
        msg.sender_last_name || ''
      }`.trim()

      sendToClient(ws, {
        id: msg.id,

        type: 1,

        sendType: 'totalMsg',

        sender_id: Number(msg.sender_id),

        receiver_id: Number(msg.group_id),

        reciever_id: Number(msg.group_id),

        group_id: Number(msg.group_id),

        group_name: msg.group_name || '',

        sender_name: senderName || 'Unknown',

        receiver_name: null,

        reciever_name: null,

        content: msg.message_text,

        image_url: msg.image_url || null,

        sent_time: msg.sent_time,

        is_read: msg.is_read,

        sender: Number(msg.sender_id)
      })
    })
  })
}

// =====================================================
// UPDATE READ STATUS
// =====================================================

function updateReadStatus (ws, receiverId, isGroup) {
  if (!isSocketAuthenticated(ws)) {
    return
  }

  const userId = Number(ws.userId)

  const targetId = Number(receiverId)

  if (!userId || !targetId) {
    sendToClient(ws, {
      type: 'error',

      sendType: 'update_read_status_error',

      message: 'Invalid receiver ID'
    })

    return
  }

  let query

  let params

  if (isGroup) {
    query = `
      UPDATE user_message

      SET is_read = 1

      WHERE
        type = 1

        AND group_id = ?

        AND sender_id != ?

        AND (
          is_read = 0
          OR is_read IS NULL
        )
    `

    params = [targetId, userId]
  } else {
    query = `
      UPDATE user_message

      SET is_read = 1

      WHERE
        type = 0

        AND reciever_id = ?

        AND sender_id = ?

        AND (
          is_read = 0
          OR is_read IS NULL
        )
    `

    params = [userId, targetId]
  }

  db.query(query, params, (err, result) => {
    if (err) {
      console.error('Update read status error:', err)

      sendToClient(ws, {
        type: 'error',

        sendType: 'update_read_status_error',

        message: 'Failed to update read status'
      })

      return
    }

    sendToClient(ws, {
      type: 'success',

      sendType: 'message_read_status',

      user_id: userId,

      receiver_id: targetId,

      group_id: isGroup ? targetId : 0,

      type: isGroup ? 1 : 0,

      isGroup
    })
  })
}

// =====================================================
// WEBSOCKET CONNECTION
// =====================================================

wss.on('connection', ws => {
  ws.userId = null

  ws.accessToken = null

  ws.authenticated = false

  ws.authenticatedAt = null

  // Prevent multiple simultaneous auth requests.
  ws.authenticating = false

  console.log('WebSocket client connected')

  // ===================================================
  // MESSAGE
  // ===================================================

  ws.on('message', async message => {
    try {
      const rawMessage = message.toString().trim()

      if (!rawMessage) {
        return
      }

      let data

      try {
        data = JSON.parse(rawMessage)
      } catch (error) {
        sendToClient(ws, {
          type: 'error',

          sendType: 'invalid_json',

          message: 'Invalid JSON'
        })

        return
      }

      // =================================================
      // AUTHENTICATION
      // =================================================

      if (data.sendType === 'auth') {
        // Prevent multiple simultaneous auth calls.
        if (ws.authenticating === true) {
          sendToClient(ws, {
            type: 'error',

            sendType: 'auth_in_progress',

            authenticated: false,

            message: 'Authentication is already in progress'
          })

          return
        }

        ws.authenticating = true

        try {
          const authenticated = await authenticateSocket(ws, data)

          if (!authenticated) {
            return
          }

          // =================================================
          // LOAD PREVIOUS CHAT AFTER AUTH
          // =================================================

          if (data.receiverId || data.recieverId) {
            const receiverId = Number(data.receiverId || data.recieverId)

            const isGroup = Boolean(data.isGroup)

            getPreviousMessages(ws, ws.userId, receiverId, isGroup)
          }
        } finally {
          ws.authenticating = false
        }

        return
      }

      // =================================================
      // AUTH REQUIRED
      // =================================================

      if (!requireAuthentication(ws)) {
        return
      }

      const authenticatedUserId = Number(ws.userId)

      // =================================================
      // USER INFO
      // =================================================

      if (data.sendType === 'userInfo') {
        sendUserInfo(ws, Number(data.masterId))

        return
      }

      // =================================================
      // TOTAL UNREAD MESSAGES
      // =================================================

      if (data.sendType === 'totalMsg') {
        sendTotalUnreadMessages(ws)

        return
      }

      // =================================================
      // UPDATE READ STATUS
      // =================================================

      if (data.sendType === 'update_read_status') {
        updateReadStatus(
          ws,

          Number(data.receiverId || data.recieverId || 0),

          Boolean(data.isGroup)
        )

        return
      }

      // =================================================
      // GET PREVIOUS MESSAGES
      // =================================================

      if (data.sendType === 'get_previous_messages') {
        const receiverId = Number(data.receiverId || data.recieverId || 0)

        const isGroup = Boolean(data.isGroup)

        if (!receiverId) {
          sendToClient(ws, {
            type: 'error',

            sendType: 'previous_message_error',

            message: 'Receiver ID is required'
          })

          return
        }

        getPreviousMessages(ws, authenticatedUserId, receiverId, isGroup)

        return
      }

      // =================================================
      // CREATE GROUP
      // =================================================

      if (data.sendType === 'group_create') {
        const senderId = authenticatedUserId

        const groupName = String(data.groupName || '').trim()

        const masterId = Number(data.masterId || 0)

        const masterCompanyId = Number(data.masterCompanyId || 0)

        const selectedUsers = Array.isArray(data.userSelected)
          ? data.userSelected.map(Number).filter(Boolean)
          : []

        if (!groupName) {
          sendToClient(ws, {
            type: 'error',

            sendType: 'group_create_error',

            message: 'Group name is required'
          })

          return
        }

        if (!selectedUsers.length) {
          sendToClient(ws, {
            type: 'error',

            sendType: 'group_create_error',

            message: 'At least one user is required'
          })

          return
        }

        const groupUsers = [...new Set([senderId, ...selectedUsers])]

        const groupQuery = `
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
          groupQuery,

          [groupName, masterId, masterCompanyId, senderId, 1],

          (err, result) => {
            if (err) {
              console.error('Create group error:', err)

              sendToClient(ws, {
                type: 'error',

                sendType: 'group_create_error',

                message: 'Failed to create group'
              })

              return
            }

            const groupId = result.insertId

            const insertUser = `
              INSERT INTO user_group
              (
                group_id,
                user_id,
                is_active
              )

              VALUES (?, ?, ?)
            `

            let completed = 0

            let failed = false

            groupUsers.forEach(userId => {
              db.query(
                insertUser,

                [groupId, userId, 1],

                error => {
                  if (failed) {
                    return
                  }

                  if (error) {
                    failed = true

                    console.error('User group insert error:', error)

                    sendToClient(ws, {
                      type: 'error',

                      sendType: 'group_create_error',

                      message: 'Failed to add group members'
                    })

                    return
                  }

                  completed++

                  if (completed === groupUsers.length) {
                    const groupData = {
                      id: groupId,

                      group_id: groupId,

                      type: 1,

                      group_name: groupName,

                      created_by: senderId,

                      created_at: new Date(),

                      sendType: 'group_list'
                    }

                    groupUsers.forEach(userId => {
                      sendToUser(userId, groupData)
                    })

                    sendToClient(ws, {
                      type: 'success',

                      sendType: 'group_create_success',

                      group_id: groupId,

                      group_name: groupName,

                      message: 'Group created successfully'
                    })
                  }
                }
              )
            })
          }
        )

        return
      }

      // =================================================
      // SEND MESSAGE
      // =================================================

      if (data.sendType === 'message') {
        const senderId = authenticatedUserId

        const type = Number(data.type) === 1 ? 1 : 0

        const receiverId = Number(data.receiver_id || data.reciever_id || 0)

        const content = data.content || ''

        const imageUrl = data.image_url || null

        const sentTime =
          data.sent_time ||
          moment().tz('America/Denver').format('YYYY-MM-DD HH:mm:ss Z')

        if (!receiverId) {
          sendToClient(ws, {
            type: 'error',

            sendType: 'message_error',

            message: 'Receiver ID is required'
          })

          return
        }

        // =================================================
        // GROUP MESSAGE
        // =================================================

        if (type === 1) {
          const memberQuery = `
            SELECT 1

            FROM user_group

            WHERE
              group_id = ?

              AND user_id = ?

              AND is_active = 1

            LIMIT 1
          `

          db.query(
            memberQuery,

            [receiverId, senderId],

            (memberError, members) => {
              if (memberError || !members.length) {
                sendToClient(ws, {
                  type: 'error',

                  sendType: 'message_error',

                  message: 'You are not a member of this group'
                })

                return
              }

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
                  sent_time,
                  is_read
                )

                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              `

              db.query(
                query,

                [
                  1,
                  senderId,
                  receiverId,
                  imageUrl || null,
                  content,
                  data.master_id,
                  data.master_company_id,
                  senderId,
                  sentTime,
                  0
                ],

                (err, result) => {
                  if (err) {
                    console.error('Group message insert error:', err)

                    sendToClient(ws, {
                      type: 'error',

                      sendType: 'message_error',

                      message: 'Failed to send group message'
                    })

                    return
                  }

                  getUserName(senderId, sender => {
                    const messageData = {
                      id: result.insertId,

                      type: 1,

                      sendType: 'new_message',

                      sender_id: senderId,

                      receiver_id: receiverId,

                      reciever_id: receiverId,

                      group_id: receiverId,

                      sender_name: sender?.name || 'Unknown',

                      receiver_name: null,

                      reciever_name: null,

                      content,

                      image_url: imageUrl || null,

                      sent_time: sentTime,

                      sender: senderId
                    }

                    wss.clients.forEach(client => {
                      if (!isSocketAuthenticated(client)) {
                        return
                      }

                      const clientUserId = Number(client.userId)

                      if (clientUserId === senderId) {
                        sendToClient(client, messageData)

                        return
                      }

                      db.query(
                        memberQuery,

                        [receiverId, clientUserId],

                        (memberError, members) => {
                          if (!memberError && members.length) {
                            sendToClient(client, messageData)
                          }
                        }
                      )
                    })
                  })
                }
              )
            }
          )

          return
        }

        // =================================================
        // ONE-TO-ONE MESSAGE
        // =================================================

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
            sent_time,
            is_read
          )

          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `

        db.query(
          query,

          [
            0,
            senderId,
            receiverId,
            imageUrl || null,
            content,
            data.master_id,
            data.master_company_id,
            senderId,
            sentTime,
            0
          ],

          (err, result) => {
            if (err) {
              console.error('Message insert error:')
              console.error('Code:', err.code)
              console.error('Errno:', err.errno)
              console.error('SQL State:', err.sqlState)
              console.error('Message:', err.sqlMessage)
              console.error('SQL:', err.sql)

              sendToClient(ws, {
                type: 'error',
                sendType: 'message_error',
                message: 'Failed to send message',
                err
              })

              return
            }

            getUserName(senderId, sender => {
              getUserName(receiverId, receiver => {
                const messageData = {
                  id: result.insertId,

                  type: 0,

                  sendType: 'new_message',

                  sender_id: senderId,

                  receiver_id: receiverId,

                  reciever_id: receiverId,

                  group_id: 0,

                  sender_name: sender?.name || 'Unknown',

                  receiver_name: receiver?.name || 'Unknown',

                  reciever_name: receiver?.name || 'Unknown',

                  content,

                  image_url: imageUrl || null,

                  sent_time: sentTime,

                  sender: senderId
                }

                sendToUser(senderId, messageData)

                sendToUser(receiverId, messageData)
              })
            })
          }
        )

        return
      }

      // =================================================
      // UNKNOWN SEND TYPE
      // =================================================

      sendToClient(ws, {
        type: 'error',

        sendType: 'unknown_send_type',

        message: `Unknown sendType: ${data.sendType}`
      })
    } catch (error) {
      console.error('WebSocket message handler error:', error)

      sendToClient(ws, {
        type: 'error',

        sendType: 'server_error',

        message: 'Internal WebSocket server error'
      })
    }
  })

  // =====================================================
  // SOCKET CLOSE
  // =====================================================

  ws.on('close', (code, reason) => {
    console.log(
      `WebSocket disconnected | user=${
        ws.userId || 'Unknown'
      } | code=${code} | reason=${reason?.toString() || 'none'}`
    )

    ws.authenticated = false

    ws.userId = null

    ws.accessToken = null
  })

  // =====================================================
  // SOCKET ERROR
  // =====================================================

  ws.on('error', error => {
    console.error(`WebSocket error for user ${ws.userId || 'Unknown'}:`, error)
  })
})

// =====================================================
// PERIODIC TOKEN REVALIDATION
// =====================================================

const TOKEN_REVALIDATION_INTERVAL = 5 * 60 * 1000

setInterval(async () => {
  for (const ws of wss.clients) {
    if (!isSocketAuthenticated(ws)) {
      continue
    }

    try {
      const tokenResult = await checkAccessToken(ws.accessToken)

      // =================================================
      // TOKEN EXPIRED / INVALID
      // =================================================

      if (!tokenResult.valid) {
        ws.authenticated = false

        ws.userId = null

        ws.accessToken = null

        sendToClient(ws, {
          type: 'error',

          sendType: 'auth_expired',

          authenticated: false,

          statusCode: tokenResult.statusCode || 401,

          message: tokenResult.message || 'Session expired'
        })

        // IMPORTANT:
        // Do not close WebSocket.
        continue
      }

      // =================================================
      // USER MISMATCH
      // =================================================

      if (Number(tokenResult.userId) !== Number(ws.userId)) {
        ws.authenticated = false

        ws.userId = null

        ws.accessToken = null

        sendToClient(ws, {
          type: 'error',

          sendType: 'auth_mismatch',

          authenticated: false,

          statusCode: 401,

          message: 'Authentication mismatch'
        })

        // IMPORTANT:
        // Do not close WebSocket.
      }
    } catch (error) {
      console.error('Periodic token validation error:', error)
    }
  }
}, TOKEN_REVALIDATION_INTERVAL)

// =====================================================
// HEALTH CHECK
// =====================================================

app.get('/', (req, res) => {
  res.status(200).json({
    status: 'success',

    message: 'WebSocket server is running',

    port: PORT,

    connectedClients: wss.clients.size
  })
})

// =====================================================
// DUTY STATUS
// =====================================================

app.post('/broadcast-duty-status', (req, res) => {
  try {
    const data = req.body

    const driverId = Number(data.driverId)

    if (!driverId) {
      return res.status(400).json({
        status: 'failure',

        message: 'Valid driverId is required'
      })
    }

    const sent = sendToUser(driverId, {
      sendType: 'change-duty-status',

      driverId,

      driver: data.driver,

      vehicle: data.vehicle,

      shiftStatus: data.shiftStatus,

      startLogTime: data.startLogTime,

      endLogTime: data.endLogTime,

      duration: data.duration,

      locationName: data.locationName,

      shift_time: data.shift_time,

      cycle_time: data.cycle_time,

      break_time: data.break_time,

      drive_time: data.drive_time,

      odometer: data.odometer,

      engineHours: data.engineHours
    })

    return res.status(200).json({
      status: 'success',

      message: sent
        ? 'Duty status sent successfully'
        : 'Driver is not connected',

      driverId
    })
  } catch (error) {
    console.error('Duty status error:', error)

    return res.status(500).json({
      status: 'failure',

      message: 'Failed to broadcast duty status'
    })
  }
})

// =====================================================
// FORCE LOGOUT
// =====================================================

app.post('/broadcast-force-logout', (req, res) => {
  try {
    const driverId = Number(req.body.driverId)

    if (!driverId) {
      return res.status(400).json({
        status: 'failure',

        message: 'Valid driverId is required'
      })
    }

    const sent = closeUserSockets(
      driverId,

      'You have been logged out by the administrator'
    )

    return res.status(200).json({
      status: 'success',

      message: sent ? 'User logout successfully' : 'User is not connected',

      driverId
    })
  } catch (error) {
    console.error('Force logout error:', error)

    return res.status(500).json({
      status: 'failure',

      message: 'Failed to force logout'
    })
  }
})

// =====================================================
// HTTP SERVER
// =====================================================

const server = http.createServer(app)

// =====================================================
// WEBSOCKET UPGRADE
// =====================================================

server.on('upgrade', (request, socket, head) => {
  try {
    wss.handleUpgrade(request, socket, head, ws => {
      wss.emit('connection', ws, request)
    })
  } catch (error) {
    console.error('WebSocket upgrade error:', error)

    socket.destroy()
  }
})

// =====================================================
// START
// =====================================================

server.listen(PORT, '0.0.0.0', () => {
  console.log(`HTTP + WebSocket server running on port ${PORT}`)

  console.log(`[CONFIG] BACKEND_URL=${process.env.BACKEND_URL || '(missing)'}`)
})
